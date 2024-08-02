/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.pgsql.connection;

import io.debezium.connector.pgsql.PostgresConnectorConfig;
import io.debezium.connector.pgsql.PostgresSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final PostgresConnectorConfig config;
    private final PostgresSchema schema;

    public MessageDecoderContext(PostgresConnectorConfig config, PostgresSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public PostgresConnectorConfig getConfig() {
        return config;
    }

    public PostgresSchema getSchema() {
        return schema;
    }
}
