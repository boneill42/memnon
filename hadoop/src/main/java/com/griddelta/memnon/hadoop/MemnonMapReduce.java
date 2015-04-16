package com.griddelta.memnon.hadoop;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class MemnonMapReduce extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(MemnonMapReduce.class);
    public static final String RUBY_CODE = "griddelta.memnon.rubyCode";

    public static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MemnonMapReduce(), args);
        System.exit(0);
    }

    public static class RubyMapper extends Mapper<Long, Row, Text, Text> {
        private RubyRunner rubyRunner;

        @Override               
        public void setup(Context context) throws IOException, InterruptedException {
            String rubyCode = context.getConfiguration().get(RUBY_CODE);
            LOG.debug("Loading ruby code [{}]", rubyCode.hashCode());
            rubyRunner = new RubyRunner(rubyCode, new ArrayList<URL>());
        }

        public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
            Object result = rubyRunner.runMap(row);
            List<String> keys = ResultHelper.getKeys(result);
            List<String> values = ResultHelper.getValues(result);
            for (int i = 0; i < keys.size(); i++) {
                context.write(new Text(keys.get(i)), new Text(values.get(i)));
            }
        }
    }

    public static class ReducerToCassandra extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        private RubyRunner rubyRunner;
        
        @Override               
        public void setup(Context context) throws IOException, InterruptedException {
            String rubyCode = context.getConfiguration().get(RUBY_CODE);
            this.rubyRunner = new RubyRunner(rubyCode, new ArrayList<URL>());
            LOG.debug("Loading ruby code [{}]", rubyCode.hashCode());
            LOG.debug("rubyRunner = [{}]", this.rubyRunner);
        }

        public void reduce(Text key, Iterable<Text> valuesToReduce, Context context) throws IOException, InterruptedException {
            LOG.debug("rubyRunner = [{}]", this.rubyRunner);
            Map<Object, Object> output = rubyRunner.runReduce(key.toString(), valuesToReduce);
            
            for (Map.Entry<Object, Object> entry : output.entrySet()){
                Map<String, ByteBuffer> columnKeys = new HashMap<String, ByteBuffer>();  
                columnKeys.put("word", ByteBufferUtil.bytes(entry.getKey().toString()));;

                List<ByteBuffer> columnValues = new ArrayList<ByteBuffer>();
                columnValues.add(ByteBufferUtil.bytes(entry.getValue().toString()));     
                context.write(columnKeys, columnValues);                
            }
        }
    }

    public int run(String[] args) throws Exception {
        RubyRun run = RubyRun.fromArgs(args);
        LOG.debug(run.toString());
        String rubyCode = MemnonMapReduce.readFile(run.rubyCode, Charset.defaultCharset());
        
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "memnon-job");
        job.setJarByClass(MemnonMapReduce.class);        
        
        // Configure Map
        job.setMapperClass(RubyMapper.class);
        job.setInputFormatClass(CqlInputFormat.class);
        CqlConfigHelper.setInputCql(job.getConfiguration(), "select * from " + run.inputColumnFamily + " where token(" + run.inputPartitionKey + ") > ? and token("
                + run.inputPartitionKey + ") <= ? allow filtering");        
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), run.cassandraHost);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), run.inputKeyspace, run.inputColumnFamily);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setInputSplitSize(job.getConfiguration(), run.splitSize);
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), Integer.toString(run.pageSize));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Configure Reduce
        //job.setCombinerClass(ReducerToCassandra.class);
        job.setReducerClass(ReducerToCassandra.class);
        job.setOutputKeyClass(Map.class); 
        job.setOutputValueClass(List.class);   
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), run.cassandraHost);
        job.setOutputFormatClass(CqlOutputFormat.class);
        job.getConfiguration().setIfUnset("row_key", "word"); 
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), run.outputKeyspace, run.outputColumnFamily);
        CqlConfigHelper.setOutputCql(job.getConfiguration(), "update " + run.outputColumnFamily + " set sum = ?");        
        job.getConfiguration().set(RUBY_CODE, rubyCode);
        job.waitForCompletion(true);
        return 0;
    }
}