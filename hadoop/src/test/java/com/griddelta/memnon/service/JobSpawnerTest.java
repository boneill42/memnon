package com.griddelta.memnon.service;

import org.junit.Test;

import com.griddelta.memnon.hadoop.RubyRun;

public class JobSpawnerTest {

    @Test
    public void testMapReduceLocal() throws Exception {
        RubyRun.withConfig().withCassandraHost("localhost").withJobName("word_count")
                .withInputKeyspace("library").withInputColumnFamily("books").withInputPartitionKey("title")
                .withOutputKeyspace("library").withOutputColumnFamily("word_count").withOutputPartitionKey("word")
                .withSplitSize(10000).withPageSize(100)
                .withRubyCode("src/test/resources/wordcount.rb").spawnLocal();
    }
}
