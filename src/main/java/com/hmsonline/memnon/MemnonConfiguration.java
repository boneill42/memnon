package com.hmsonline.memnon;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.datastax.driver.core.ConsistencyLevel;
import com.yammer.dropwizard.config.Configuration;

public class MemnonConfiguration extends Configuration {
    public final static String CASSANDRA_HOST_PROPERTY = "virgil.cassandra_host";
    public final static String CASSANDRA_PORT_PROPERTY = "virgil.cassandra_port";
    public final static String CASSANDRA_EMBEDDED = "virgil.embedded";

    @NotEmpty
    @NotNull
    private String solrHost;

    @NotEmpty
    @NotNull
    private String cassandraYaml;

    private boolean enableIndexing;

    public String getSolrHost() {
        return solrHost;
    }

    public String getCassandraYaml() {
        return cassandraYaml;
    }

    public boolean isIndexingEnabled() {
        return enableIndexing;
    }

    public ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
        // Defaulting consistency level to QUORUM
        if (consistencyLevel == null)
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.valueOf(consistencyLevel);
    }

    public static boolean isEmbedded() {
        if (System.getProperty(MemnonConfiguration.CASSANDRA_EMBEDDED) == null)
            return true;
        else
            return (System.getProperty(MemnonConfiguration.CASSANDRA_EMBEDDED).equals("1"));
    }

    public static String getHost() {
        return System.getProperty(MemnonConfiguration.CASSANDRA_HOST_PROPERTY);
    }

    public static int getPort() {
        return Integer.parseInt(System.getProperty(MemnonConfiguration.CASSANDRA_PORT_PROPERTY));
    }
}