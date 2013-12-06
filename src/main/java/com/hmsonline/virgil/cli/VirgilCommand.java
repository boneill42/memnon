package com.hmsonline.virgil.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.hmsonline.virgil.CassandraStorage;
import com.hmsonline.virgil.VirgilService;
import com.hmsonline.virgil.config.VirgilConfiguration;
import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.ServerCommand;

public class VirgilCommand extends ServerCommand<VirgilConfiguration> {
    public VirgilCommand(String name) {
        super(VirgilConfiguration.class);
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

    private CassandraStorage createCassandraStorage(CommandLine params, VirgilConfiguration config) throws Exception {

        String cassandraHost = params.getOptionValue("host");
        if (cassandraHost == null)
            throw new RuntimeException("Need to specify a host.");
        String cassandraPort = params.getOptionValue("port");
        if (cassandraPort == null)
            cassandraPort = "9160";
        System.setProperty(VirgilConfiguration.CASSANDRA_HOST_PROPERTY, cassandraHost);
        System.setProperty(VirgilConfiguration.CASSANDRA_PORT_PROPERTY, cassandraPort);
        System.out.println("Starting virgil against remote cassandra server [" + cassandraHost + ":" + cassandraPort
                + "]");
        return new CassandraStorage(config);
    }

    @Override
    protected void run(AbstractService<VirgilConfiguration> service, VirgilConfiguration config, CommandLine params)
            throws Exception {
        assert (service instanceof VirgilService);
        VirgilService virgil = (VirgilService) service;
        CassandraStorage storage = this.createCassandraStorage(params, config);
        virgil.setStorage(storage);
        virgil.setConfig(config);
        super.run(service, config, params);
    }
}
