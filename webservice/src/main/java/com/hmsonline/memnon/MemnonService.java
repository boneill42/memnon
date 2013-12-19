package com.hmsonline.memnon;


import com.hmsonline.memnon.exception.KeyspaceExceptionMapper;
import com.hmsonline.memnon.health.CassandraHealthCheck;
import com.hmsonline.memnon.resource.DataResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.bundles.AssetsBundle;
import com.yammer.dropwizard.config.Environment;

public class MemnonService extends Service<MemnonConfiguration> {
    public static CassandraStorage storage = null;
    MemnonConfiguration config = null;
    
    public static void main(String[] args) throws Exception {
        new MemnonService().run(args);
    }

    protected MemnonService() {
        super("memnon");
        addBundle(new AssetsBundle("/ui", "/"));
        addCommand(new MemnonCommand("cassandra"));
    }

    @Override
    protected void initialize(MemnonConfiguration conf, Environment env) throws Exception {
        env.addResource(new DataResource(this));
        env.addHealthCheck(new CassandraHealthCheck(this));
        env.addProvider(new KeyspaceExceptionMapper());
    }

    public CassandraStorage getStorage() {
        return storage;
    }

    public void setStorage(CassandraStorage storage) {
        MemnonService.storage = storage;
    }

    public MemnonConfiguration getConfig() {
        return config;
    }

    public void setConfig(MemnonConfiguration config) {
        this.config = config;
    }
}
