package io.debezium.connector.pgsql;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;

/**
 * @author twan
 * @version 1.0
 * @description
 * @date 2024-06-21 18:55:31
 */
public class WALMinerCDCTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALMinerCDCTest.class);

    private static DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.setProperty("name", "dbz-engine");
        props.setProperty("connector.class", "io.debezium.connector.pgsql.PostgresConnector");

        // offset config begin - 使用文件来存储已处理的binlog偏移量
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "D:\\qidian\\data\\oracle_offsets.dat");
        props.setProperty("offset.flush.interval.ms", "0");
        // offset config end
        props.setProperty("database.server.name", "test01");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "D:\\qidian\\data\\oracle_dbhistory.txt");
        props.setProperty("database.hostname", "hdp05");
        props.setProperty("database.dbname", "postgres");
        props.setProperty("database.port", "9001");
        props.setProperty("database.user", "postgres");
        props.setProperty("database.password", "postgres");
        props.setProperty("table.include.list", "public.students");// 要捕获的数据表
        props.setProperty("snapshot.mode", "initial");// 全量+增量

        // props.setProperty("snapshot.mode", "schema_only");
        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying(t -> {
                    try {
                        handleChangeEvent(t);
                    }
                    catch (JsonProcessingException e) {
                        e.printStackTrace();
                        LOGGER.error("create engine error:{}", e);
                    }
                })
                .using((success, message, error) -> {
                    if (error != null) {
                        // 报错回调
                        error.printStackTrace();
                        System.out.println("------------error, message:" + message + "exception:" + error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        addShutdownHook(engine);
        awaitTermination(executor);
        System.out.println("------------main finished.");
    }

    private static void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordChangeEvent) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        SourceRecord sourceRecord = sourceRecordChangeEvent.record();
        Struct sourceRecordChangeValue = (Struct) sourceRecord.value();

        try {
            Envelope.Operation operation = Envelope.Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));
            String record = operation == Envelope.Operation.DELETE ? BEFORE : AFTER;
            Struct struct = (Struct) sourceRecordChangeValue.get(record);
            // 封装变更数据为map
            Struct finalStruct = struct;
            Struct finalStruct1 = struct;
            Map<String, Object> payload = struct.schema().fields().stream()
                    .map(Field::name)
                    .filter(fieldName -> finalStruct.get(fieldName) != null)
                    .map(fieldName -> Pair.of(fieldName, finalStruct1.get(fieldName)))
                    .collect(toMap(Pair::getKey, Pair::getValue));
            Struct dbTb = (Struct) sourceRecordChangeValue.get("source");
            String db = dbTb.get("db").toString();
            String tb = dbTb.get("table").toString();
            LOGGER.info(db + "." + tb + "表，数据为: " + payload);
        }
        catch (Exception e) {
            Struct struct = (Struct) sourceRecord.value();
            Map<String, Object> payload = struct.schema().fields().stream()
                    .map(Field::name)
                    .filter(fieldName -> struct.get(fieldName) != null)
                    .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                    .collect(toMap(Pair::getKey, Pair::getValue));
            LOGGER.info("payload:{}", payload);
            LOGGER.info("ddl:{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
        }

    }

    private static void closeEngine(DebeziumEngine<RecordChangeEvent<SourceRecord>> engine) {
        try {
            engine.close();
        }
        catch (IOException ignored) {
        }
    }

    private static void addShutdownHook(DebeziumEngine<RecordChangeEvent<SourceRecord>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(engine)));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
