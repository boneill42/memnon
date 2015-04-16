package com.griddelta.memnon.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.jruby.RubyArray;
import org.jruby.RubyHash;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.griddelta.memnon.hadoop.MemnonMapReduce;
import com.griddelta.memnon.hadoop.ResultHelper;
import com.griddelta.memnon.hadoop.RubyRunner;
import com.griddelta.memnon.service.mock.MockRow;

public class RubyRunnerTest {
    private static final Logger logger = LoggerFactory.getLogger(RubyRunnerTest.class);

    @Test
    public void testWordCount() throws IOException {
        logger.debug("Testing word count.");
        String rubyCode = MemnonMapReduce.readFile("src/test/resources/wordcount.rb", Charset.defaultCharset());        
        RubyRunner rubyRunner = new RubyRunner(rubyCode, new ArrayList<URL>());
        Row mockRow = new MockRow(new String[] { "title", "content" }, new String[] { "buff", 
                "buffalo in buffalo buffalo buffalo buffalo in cleveland" });
        Object mapOutput = rubyRunner.runMap(mockRow);
        assertValidResult(mapOutput);
        List<String> keys = ResultHelper.getKeys(mapOutput);
        List<String> values = ResultHelper.getValues(mapOutput);
        assertEquals(8, keys.size());
        assertEquals("buffalo", keys.get(0));
        assertEquals(8, values.size());
        assertEquals("1", values.get(0));
        
        List<Text> reduceValues = new ArrayList<Text>();
        reduceValues.add(new Text("1"));
        reduceValues.add(new Text("1"));
        reduceValues.add(new Text("1"));
        reduceValues.add(new Text("1"));        
        RubyHash reduceOutput = (RubyHash) rubyRunner.runReduce("buffalo", reduceValues);        
        assertNotNull(reduceOutput);
        assertEquals("4", reduceOutput.get("buffalo"));        
    }

    private void assertValidResult(Object obj) {
        RubyArray result = (RubyArray) obj;
        assertNotNull(result);
        assertEquals(2, result.size());
    }
}
