package com.griddelta.memnon;

import io.dropwizard.Configuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.datastax.driver.core.ConsistencyLevel;

public class MemnonConfiguration extends Configuration {
    @NotEmpty
    @NotNull
    private String host;

    @Min(1)
    @Max(65535)
    private int port = 9160;

    public String getHost() {
        return host;
    }

    public int getPort() {
       return port;
    }

    public ConsistencyLevel getConsistencyLevel(){
        // TOOD: Make this configurable
        return ConsistencyLevel.QUORUM;
    }
}