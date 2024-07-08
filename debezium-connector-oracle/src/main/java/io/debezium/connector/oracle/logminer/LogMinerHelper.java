/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.DelayStrategy;
import io.debezium.util.Strings;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * This method substitutes CONTINUOUS_MINE functionality
     *
     * @param connection connection
     * @param lastProcessedScn current offset
     * @param archiveLogRetention the duration that archive logs will be mined
     * @param archiveLogOnlyMode true to mine only archive lgos, false to mine all available logs
     * @param archiveDestinationName configured archive log destination name to use, may be {@code null}
     * @param maxRetries the number of retry attempts before giving up and throwing an exception about log state
     * @param initialDelay the initial delay
     * @param maxDelay the maximum delay
     * @throws SQLException if anything unexpected happens
     * @return log files that were added to the current mining session
     */
    // todo: check RAC resiliency
    public static List<LogFile> setLogFilesForMining(OracleConnection connection,OracleConnection miningJdbcConnection, Scn lastProcessedScn, Duration archiveLogRetention,
                                                     boolean archiveLogOnlyMode, String archiveDestinationName, int maxRetries,
                                                     Duration initialDelay, Duration maxDelay)
            throws SQLException {
        removeLogFilesFromMining(miningJdbcConnection);
        // Restrict max attempts to 0 or greater values (sanity-check)
        // the code will do at least 1 attempt and up to maxAttempts extra polls based on configuration
        final int maxAttempts = Math.max(maxRetries, 0);
        final DelayStrategy retryStrategy = DelayStrategy.exponential(initialDelay.toMillis(), maxDelay.toMillis());

        // We perform a retry algorithm here as there is a race condition where Oracle may update the V$LOG table
        // but the V$ARCHIVED_LOG lags behind and a single-shot SQL query may return an inconsistent set of results
        // due to Oracle performing the operation non-atomically.
        List<LogFile> logFilesForMining = new ArrayList<>();
        for (int attempt = 0; attempt <= maxAttempts; ++attempt) {
            logFilesForMining.addAll(getLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention,
                    archiveLogOnlyMode, archiveDestinationName));
            // we don't need lastProcessedSCN in the logs, as that one was already processed, but we do want
            // the next SCN to be present, as that is where we'll start processing from.
            if (!hasLogFilesStartingBeforeOrAtScn(logFilesForMining, lastProcessedScn.add(Scn.ONE))) {
                LOGGER.info("No logs available yet (attempt {})...", attempt + 1);
                logFilesForMining.clear();
                retryStrategy.sleepWhen(true);
                continue;
            }

            List<String> logFilesNames = logFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
            for (String file : logFilesNames) {
                LOGGER.trace("Adding log file {} to mining session", file);
                String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
                LOGGER.debug("debug sql:{}",addLogFileStatement);
                try {
                    executeCallableStatement(miningJdbcConnection, addLogFileStatement);
                } catch (SQLException e) {
                    if (e.getMessage().contains("ORA-01289")){
                        LOGGER.warn("add file error:{}",addLogFileStatement);
                    }else{
                        throw e;
                    }
                }
            }

            LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}", lastProcessedScn, logFilesNames);
            return logFilesForMining;
        }

        final Scn minScn = getMinimumScn(logFilesForMining);
        if ((minScn.isNull() || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
            throw new DebeziumException("The log.mining.archive.log.only mode was recently enabled and the offset SCN " +
                    lastProcessedScn + "is not yet in any available archive logs. " +
                    "Please perform an Oracle log switch and restart the connector.");
        }
        throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
    }

    private static boolean hasLogFilesStartingBeforeOrAtScn(List<LogFile> logs, Scn scn) {
        return logs.stream().anyMatch(l -> l.getFirstScn().compareTo(scn) <= 0);
    }

    private static Scn getMinimumScn(List<LogFile> logs) {
        return logs.stream().map(LogFile::getFirstScn).min(Scn::compareTo).orElse(Scn.NULL);
    }

    static void logWarn(OracleStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.warn(format, args);
        streamingMetrics.incrementWarningCount();
    }

    static void logError(OracleStreamingChangeEventSourceMetrics streamingMetrics, String format, Object... args) {
        LOGGER.error(format, args);
        streamingMetrics.incrementErrorCount();
    }

    /**
     * Get all log files that should be mined.
     *
     * @param connection database connection
     * @param offsetScn offset system change number
     * @param archiveLogRetention duration that archive logs should be mined
     * @param archiveLogOnlyMode true to mine only archive logs, false to mine all available logs
     * @param archiveDestinationName archive destination to use, may be {@code null}
     * @return list of log files
     * @throws SQLException if a database exception occurs
     */
    public static List<LogFile> getLogFilesForOffsetScn(OracleConnection connection, Scn offsetScn, Duration archiveLogRetention,
                                                        boolean archiveLogOnlyMode, String archiveDestinationName)
            throws SQLException {
        LOGGER.trace("Getting logs to be mined for offset scn {}", offsetScn);

        final List<LogFile> logFiles = new ArrayList<>();
        final Set<LogFile> onlineLogFiles = new LinkedHashSet<>();
        final Set<LogFile> archivedLogFiles = new LinkedHashSet<>();
        // String minableLogsQuery = SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
        String queryStandbyRedoLogsSql = "SELECT\n" +
                "    MIN(F.MEMBER) AS FILE_NAME,\n" +
                "    L.FIRST_CHANGE# AS FIRST_CHANGE,\n" +
                "    L.NEXT_CHANGE# AS NEXT_CHANGE,\n" +
                "\t\tL.ARCHIVED,\n" +
                "    'CURRENT' as STATUS,\n" +
                "    'ONLINE' AS TYPE,\n" +
                "    L.SEQUENCE# AS SEQ,\n" +
                "    'NO' AS DICT_START,\n" +
                "    'NO' AS DICT_END,\n" +
                "    L.THREAD# AS THREAD\n" +
                "FROM\n" +
                "    V$LOGFILE F,\n" +
                "    V$STANDBY_LOG L\n" +
                "WHERE\n" +
                "    F.GROUP# = L.GROUP# and  (L.ARCHIVED='NO' or L.STATUS='ACTIVE')\n" +
                "GROUP BY\n" +
                "    F.GROUP#,\n" +
                "    L.FIRST_CHANGE#,\n" +
                "    L.NEXT_CHANGE#,\n" +
                "    L.STATUS,\n" +
                "\t\tL.ARCHIVED,\n" +
                "    L.SEQUENCE#,\n" +
                "    L.THREAD# ";
        JdbcConfiguration config = connection.config();
        String archivelogPathConvert = config.getString("archivelog_path_convert");
        String redoPathConvert = config.getString("redo_path_convert");
        StringBuilder sb = new StringBuilder(queryStandbyRedoLogsSql);
        sb.append(" union ");
        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, ");
        sb.append("'YES' as  ARCHIVED, NULL as  STATUS, 'ARCHIVED' as TYPE, A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN as  DICT_START, ");
        sb.append("A.DICTIONARY_END as DICT_END, A.THREAD# AS THREAD FROM V$ARCHIVED_LOG A ");
        sb.append("WHERE A.NAME IS NOT NULL AND A.ARCHIVED = 'YES' AND A.STATUS = 'A'");
        sb.append("AND A.NEXT_CHANGE# > ").append(offsetScn).append(" ");
        ;
        sb.append("AND A.DEST_ID IN (").append(" SELECT DEST_ID FROM v$archive_dest_status WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1").append(") ");
        LOGGER.debug("sql:{}",sb.toString());
        connection.query(sb.toString(), rs -> {
            while (rs.next()) {
                String originFileName = rs.getString("FILE_NAME");
                // String newFileName=filePath.substring(filePath.lastIndexOf("/")+1,filePath.indexOf("."))+"_standby.log";//转换
                String fileName = "";
                if(originFileName.endsWith(".log")&&!StringUtil.isNullOrEmpty(redoPathConvert)){
                    String[] strings = redoPathConvert.split(":");
                    fileName = originFileName.replace(strings[0], strings[1]);
                } else if(!StringUtil.isNullOrEmpty(archivelogPathConvert)) {
                    String[] strings = archivelogPathConvert.split(":");
                    fileName = originFileName.replace(strings[0], strings[1]);
                }else{
                    fileName=originFileName;
                }
                Scn firstScn = getScnFromString(rs.getString(2));
                Scn nextScn = getScnFromString(rs.getString(3));
                String status = rs.getString(5);
                String type = rs.getString(6);
                BigInteger sequence = new BigInteger(rs.getString(7));
                int thread = rs.getInt(10);
                if ("ARCHIVED".equals(type)) {
                    // archive log record
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE, thread);
                    if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.trace("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
                        archivedLogFiles.add(logFile);
                    }
                }
                else if ("ONLINE".equals(type)) {
                    // LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status), thread);
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, "CURRENT".equalsIgnoreCase(status), thread);
                    if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
                        onlineLogFiles.add(logFile);
                    }
                    else {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
                    }
                }
            }
        });

        /*
         * conn.query(SqlUtils.allMinableLogsQuery(offsetScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName), rs -> {
         * while (rs.next()) {
         * String fileName = rs.getString(1);
         * Scn firstScn = getScnFromString(rs.getString(2));
         * Scn nextScn = getScnFromString(rs.getString(3));
         * String status = rs.getString(5);
         * String type = rs.getString(6);
         * BigInteger sequence = new BigInteger(rs.getString(7));
         * int thread = rs.getInt(10);
         * if ("ARCHIVED".equals(type)) {
         * // archive log record
         * LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE, thread);
         * if (logFile.getNextScn().compareTo(offsetScn) >= 0) {
         * LOGGER.trace("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
         * archivedLogFiles.add(logFile);
         * }
         * }
         * else if ("ONLINE".equals(type)) {
         * LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status), thread);
         * if (logFile.isCurrent() || logFile.getNextScn().compareTo(offsetScn) >= 0) {
         * LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
         * onlineLogFiles.add(logFile);
         * }
         * else {
         * LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
         * }
         * }
         * }
         * });
         */

        // DBZ-3563
        // To avoid duplicate log files (ORA-01289 cannot add duplicate logfile)
        // Remove the archive log which has the same sequence number.
        for (LogFile redoLog : onlineLogFiles) {
            archivedLogFiles.removeIf(f -> {
                if (f.getSequence().equals(redoLog.getSequence())) {
                    LOGGER.trace("Removing archive log {} with duplicate sequence {} to {}", f.getFileName(), f.getSequence(), redoLog.getFileName());
                    return true;
                }
                return false;
            });
        }
        logFiles.addAll(archivedLogFiles);
        logFiles.addAll(onlineLogFiles);

        return logFiles;
    }

    private static Scn getScnFromString(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return Scn.MAX;
        }
        return Scn.valueOf(value);
    }

    /**
     * This method removes all added log files from mining
     * @param conn connection
     * @throws SQLException something happened
     */
    public static void removeLogFilesFromMining(OracleConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.connection(false).prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS");
                ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(conn, SqlUtils.deleteLogFileStatement(fileName));
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        }
    }

    private static void executeCallableStatement(OracleConnection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        LOGGER.debug("sql:{}",statement);

        try (CallableStatement s = connection.connection(false).prepareCall(statement)) {
            s.execute();
        }
    }

    /**
     * Returns a 0-based index offset for the column name in the relational table.
     *
     * @param columnName the column name, should not be {@code null}.
     * @param table the relational table, should not be {@code null}.
     * @return the 0-based index offset for the column name
     */
    public static int getColumnIndexByName(String columnName, Table table) {
        final Column column = table.columnWithName(columnName);
        if (column == null) {
            throw new DebeziumException("No column '" + columnName + "' found in table '" + table.id() + "'");
        }
        // want to return a 0-based index and column positions are 1-based
        return column.position() - 1;
    }
}
