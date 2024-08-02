/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.pgsql;

import java.sql.SQLException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    protected final static Logger LOGGER = LoggerFactory.getLogger(PostgresTaskContext.class);

    private final PostgresConnectorConfig config;
    private final TopicSelector<TableId> topicSelector;
    private final PostgresSchema schema;

    private ElapsedTimeStrategy refreshXmin;
    private Long lastXmin;

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema, TopicSelector<TableId> topicSelector) {
        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);

        this.config = config;
        if (config.xminFetchInterval().toMillis() > 0) {
            this.refreshXmin = ElapsedTimeStrategy.constant(Clock.SYSTEM, config.xminFetchInterval().toMillis());
        }
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
    }

    protected TopicSelector<TableId> topicSelector() {
        return topicSelector;
    }

    protected PostgresSchema schema() {
        return schema;
    }

    protected PostgresConnectorConfig config() {
        return config;
    }

    protected void refreshSchema(PostgresConnection connection, boolean printReplicaIdentityInfo) throws SQLException {
        schema.refresh(connection, printReplicaIdentityInfo);
    }

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
