package com.hmsonline.memnon;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.ServerCommand;

public class MemnonCommand extends ServerCommand<MemnonConfiguration> {
    public MemnonCommand(String name) {
        super(MemnonConfiguration.class);
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options options = new Options();
        OptionGroup runMode = new OptionGroup();
        Option host = OptionBuilder.withArgName("h").hasArg().withDescription("Host name for Cassandra.")
                .create("host");
        runMode.addOption(host);
        options.addOptionGroup(runMode);
        return options;
    }

    private CassandraStorage createCassandraStorage(CommandLine params, MemnonConfiguration config) throws Exception {

        String cassandraHost = params.getOptionValue("host");
        if (cassandraHost == null)
            throw new RuntimeException("Need to specify a host.");
        String cassandraPort = params.getOptionValue("port");
        if (cassandraPort == null)
            cassandraPort = "9160";
        System.setProperty(MemnonConfiguration.CASSANDRA_HOST_PROPERTY, cassandraHost);
        System.setProperty(MemnonConfiguration.CASSANDRA_PORT_PROPERTY, cassandraPort);
        System.out.println("Starting virgil against remote cassandra server [" + cassandraHost + ":" + cassandraPort
                + "]");
        return new CassandraStorage(config);
    }

    @Override
    protected void run(AbstractService<MemnonConfiguration> service, MemnonConfiguration config, CommandLine params)
            throws Exception {
        assert (service instanceof MemnonService);
        MemnonService virgil = (MemnonService) service;
        CassandraStorage storage = this.createCassandraStorage(params, config);
        virgil.setStorage(storage);
        virgil.setConfig(config);
        super.run(service, config, params);
    }
}
