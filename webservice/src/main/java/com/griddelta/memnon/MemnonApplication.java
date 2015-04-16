package com.griddelta.memnon;


import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.griddelta.memnon.exception.KeyspaceExceptionMapper;
import com.griddelta.memnon.health.CassandraHealthCheck;
import com.griddelta.memnon.resource.DataResource;

public class MemnonApplication extends Application<MemnonConfiguration> {
    public static CassandraStorage storage = null;
    MemnonConfiguration config = null;

    public static void main(String[] args) throws Exception {
        new MemnonApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<MemnonConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/ui", "/foo"));        
        // bootstrap.addCommand(new MemnonCommand(this));
    }

    @Override
    public void run(MemnonConfiguration configuration, Environment env) throws Exception {    	
        env.jersey().register(new DataResource(this));
        env.healthChecks().register("Cassandra", new CassandraHealthCheck(this, configuration));
        env.jersey().register(new KeyspaceExceptionMapper());
    }

    public CassandraStorage getStorage() {
        return storage;
    }

    public void setStorage(CassandraStorage storage) {
        MemnonApplication.storage = storage;
    }

    public MemnonConfiguration getConfig() {
        return config;
    }

    public void setConfig(MemnonConfiguration config) {
        this.config = config;
    }
}
