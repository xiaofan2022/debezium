/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.pgsql;

import java.sql.SQLException;
import java.util.OptionalLong;

import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.pgsql.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;

/**
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresStreamingChangeEventSource implements StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> {

    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    private final PostgresConnection connection;
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    // Offset committing is an asynchronous operation.
    // When connector is restarted we cannot be sure about timing of recovery, offset committing etc.
    // as this is driven by Kafka Connect. This might be a root cause of DBZ-5163.
    // This flag will ensure that LSN is flushed only if we are really in message processing mode.
    private volatile boolean lsnFlushingAllowed = false;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private Lsn lastCompletelyProcessedLsn;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter,
                                              PostgresConnection connection, PostgresEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                              PostgresSchema schema, PostgresTaskContext taskContext) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

    }

    @Override
    public void init() {
        // refresh the schema so we have a latest view of the DB tables
        try {
            taskContext.refreshSchema(connection, true);
        }
        catch (SQLException e) {
            throw new DebeziumException("Error while executing initial schema load", e);
        }
    }

    @Override
    public void execute(ChangeEventSourceContext context, PostgresPartition partition, PostgresOffsetContext offsetContext)
            throws InterruptedException {

    }

    private void probeConnectionIfNeeded() throws SQLException {
        if (connectionProbeTimer.hasElapsed()) {
            connection.prepareQuery("SELECT 1");
            connection.commit();
        }
    }

    private void commitMessage(PostgresPartition partition, PostgresOffsetContext offsetContext, final Lsn lsn) throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);
        maybeWarnAboutGrowingWalBacklog(false);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    /**
     * If we receive change events but all of them get filtered out, we cannot
     * commit any new offset with Apache Kafka. This in turn means no LSN is ever
     * acknowledged with the replication slot, causing any ever growing WAL backlog.
     * <p>
     * This situation typically occurs if there are changes on the database server,
     * (e.g. in an excluded database), but none of them is in table.include.list.
     * To prevent this, heartbeats can be used, as they will allow us to commit
     * offsets also when not propagating any "real" change event.
     * <p>
     * The purpose of this method is to detect this situation and log a warning
     * every {@link #GROWING_WAL_WARNING_LOG_INTERVAL} filtered events.
     *
     * @param dispatched
     *            Whether an event was sent to the broker or not
     */
    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
        else {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning++;
        }

        if (numberOfEventsSinceLastEventSentOrWalGrowingWarning > GROWING_WAL_WARNING_LOG_INTERVAL && !dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. "
                    + "This prevents the replication slot from acknowledging the processed WAL offsets, "
                    + "causing a growing backlog of non-removeable WAL segments on the database server. "
                    + "Consider to either adjust your filter configuration or enable heartbeat events "
                    + "(via the {} option) to avoid this situation.",
                    numberOfEventsSinceLastEventSentOrWalGrowingWarning, Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME);

            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
    }

    /**
     * Returns whether the current streaming phase is running a catch up streaming
     * phase that runs before a snapshot. This is useful for transaction
     * management.
     *
     * During pre-snapshot catch up streaming, we open the snapshot transaction
     * early and hold the transaction open throughout the pre snapshot catch up
     * streaming phase so that we know where to stop streaming and can start the
     * snapshot phase at a consistent location. This is opposed the regular streaming,
     * where we do not a lingering open transaction.
     *
     * @return true if the current streaming phase is performing catch up streaming
     */
    private boolean isInPreSnapshotCatchUpStreaming(PostgresOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    private Long toLong(OptionalLong l) {
        return l.isPresent() ? Long.valueOf(l.getAsLong()) : null;
    }

    private String toString(OptionalLong l) {
        return l.isPresent() ? String.valueOf(l.getAsLong()) : null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}
