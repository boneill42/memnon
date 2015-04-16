package com.griddelta.memnon.hadoop;

import java.util.ArrayList;
import java.util.List;

public class RubyRun {
    public String cassandraHost = "localhost";
    public int cassandraPort = 9042;
    public String jobName = "unknown";
    public String inputKeyspace;
    public String outputKeyspace;
    public String inputColumnFamily;
    public String outputColumnFamily;
    public String inputCql;
    public String outputCql;    
    public String inputPartitionKey;
    public String outputPartitionKey;
    public int splitSize = 1;
    public int pageSize = 10;
    public String rubyCode;

    public static RubyRun withConfig() {
        return new RubyRun();
    }

    public RubyRun withCassandraHost(String host) {
        this.cassandraHost = host;
        return this;
    }

    public RubyRun withCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
        return this;
    }

    public RubyRun withJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public RubyRun withInputKeyspace(String inputKeyspace) {
        this.inputKeyspace = inputKeyspace;
        return this;
    }

    public RubyRun withOutputKeyspace(String outputKeyspace) {
        this.outputKeyspace = outputKeyspace;
        return this;
    }

    public RubyRun withInputColumnFamily(String inputColumnFamily) {
        this.inputColumnFamily = inputColumnFamily;
        return this;
    }

    public RubyRun withOutputColumnFamily(String outputColumnFamily) {
        this.outputColumnFamily = outputColumnFamily;
        return this;
    }

    public RubyRun withInputCql(String inputCql) {
        this.inputCql = inputCql;
        return this;
    }

    public RubyRun withOutputCql(String outputCql) {
        this.outputCql = outputCql;
        return this;
    }
    
    public RubyRun withSplitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    public RubyRun withPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public RubyRun withRubyCode(String rubyCode) {
        this.rubyCode = rubyCode;
        return this;
    }

    public RubyRun withInputPartitionKey(String inputPartitionKey) {
        this.inputPartitionKey = inputPartitionKey;
        return this;
    }

    public RubyRun withOutputPartitionKey(String outputPartitionKey) {
        this.outputPartitionKey = outputPartitionKey;
        return this;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Run {\n");
        sb.append("  cassandraHost = [").append(cassandraHost).append(":").append(cassandraPort).append("]\n");
        sb.append("  jobName = [").append(jobName).append("]\n");
        sb.append("  inputKeyspace = [").append(inputKeyspace).append("]\n");
        sb.append("  inputColumnFamily = [").append(inputColumnFamily).append("]\n");
        sb.append("  outputKeyspace = [").append(outputKeyspace).append("]\n");
        sb.append("  outputColumnFamily = [").append(outputColumnFamily).append("]\n");
        sb.append("  splitSize = [").append(splitSize).append("]\n");
        sb.append("  pageSize = [").append(pageSize).append("]\n");
        sb.append("}");
        return sb.toString();

    }

    public String[] toArgs(boolean local) {
        List<String> args = new ArrayList<String>();
        if (!local) {
            args.add("lib/memnon.jar");
            args.add("com.memnon.griddelta.hadoop.TuskMapReduce");
        }
        args.add(jobName);
        args.add(cassandraHost);
        args.add(Integer.toString(cassandraPort));
        args.add(inputKeyspace);
        args.add(inputColumnFamily);
        args.add(inputCql);
        args.add(inputPartitionKey);
        args.add(outputKeyspace);
        args.add(outputColumnFamily);
        args.add(outputCql);
        args.add(outputPartitionKey);
        args.add(Integer.toString(splitSize));
        args.add(Integer.toString(pageSize));
        args.add(rubyCode);
        return args.toArray(new String[0]);
    }

    public static RubyRun fromArgs(String[] args) {
        return RubyRun.withConfig().withJobName(args[0])
                .withCassandraHost(args[1])
                .withCassandraPort(Integer.parseInt(args[2]))
                .withInputKeyspace(args[3])
                .withInputColumnFamily(args[4])
                .withInputCql(args[5])
                .withInputPartitionKey(args[6])
                .withOutputKeyspace(args[7])
                .withOutputColumnFamily(args[8])
                .withOutputCql(args[9])
                .withOutputPartitionKey(args[10])
                .withSplitSize(Integer.parseInt(args[11]))
                .withPageSize(Integer.parseInt(args[12]))
                .withRubyCode(args[13]);
    }

    public void spawnLocal() throws Exception {
        JobSpawner.spawnLocal(this);
    }

    public void spawnRemote() throws Throwable {
        JobSpawner.spawnRemote(this);
    }

}
