package com.griddelta.memnon.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSpawner {
    private static final Logger LOG = LoggerFactory.getLogger(JobSpawner.class);

    public static Configuration getConfiguration(String[] args) {
        RubyRun run = RubyRun.fromArgs(args);
        Configuration conf = new Configuration();
        conf.set("jobName", run.jobName);
        conf.set("cassandraHost",run.cassandraHost);
        conf.set("cassandraPort", Integer.toString(run.cassandraPort));
        conf.set("inputKeyspace", run.inputKeyspace);
        conf.set("inputColumnFamily", run.inputColumnFamily);
        conf.set("outputKeyspace", run.outputKeyspace);
        conf.set("outputColumnFamily", run.outputColumnFamily);
        conf.set("source", run.rubyCode);
        return conf;
    }

    public static void spawnLocal(RubyRun run)
            throws Exception {
        String[] args = run.toArgs(true);
        Configuration conf = JobSpawner.getConfiguration(args);
        ToolRunner.run(conf, new MemnonMapReduce(),args);
    }

    public static void spawnRemote(RubyRun run)
            throws Throwable {
        LOG.debug(run.toString());
        String[] args = run.toArgs(false);
        RunJar.main(args);
    }
}
