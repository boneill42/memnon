package com.hmsonline.virgil;

import static org.junit.Assert.assertEquals;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;

public class CassandraStorageTest extends VirgilServerTest {
    private static final String TABLE = "test_cf";
    private static final String KEYSPACE = "test_keyspace";
    private static Logger logger = LoggerFactory.getLogger(CassandraStorageTest.class);

    @Test
    public void testDatabaseServices() throws Exception {
        CassandraStorage dataService = VirgilService.storage;

        try { // CLEANUP FROM BEFORE
            //dataService.dropKeyspace(KEYSPACE);
        } catch (Exception e) {
            logger.warn("Ignoring exception when dropping keyspace, presuming first run.");
        }

        // CREATE KEYSPACE
        //dataService.addKeyspace(KEYSPACE, "SimpleStrategy", 1);

        // CREATE COLUMN FAMILY
        JSONObject columns = (JSONObject) JSONValue.parse("{\"col1\":\"varchar\", \"col2\":\"int\", \"col3\":\"varchar\"}");
        JSONArray keys = (JSONArray) JSONValue.parse("[\"col1\", \"col2\"]");
        //dataService.createTable(KEYSPACE, TABLE, columns, keys);

        // UPDATE
        JSONObject row = (JSONObject) JSONValue.parse("{\"col3\":\"val3\"}");
        JSONObject where = (JSONObject) JSONValue.parse("{\"col1\":\"val1\", \"col2\":2}");
        dataService.update(KEYSPACE, TABLE, row, where, ConsistencyLevel.ONE);

        // SELECT
        JSONArray selectCols = (JSONArray) JSONValue.parse("[\"col1\", \"col2\", \"col3\"]");
        JSONArray json = dataService.select(KEYSPACE, TABLE, selectCols, where, ConsistencyLevel.ONE);
        assertEquals("[[\"val1\",\"val3\"]]", json.toString());

        // PARTIAL DELETE 
        JSONArray singleCol = (JSONArray) JSONValue.parse("[\"col3\"]");
        dataService.delete(KEYSPACE, TABLE, singleCol, where, ConsistencyLevel.ONE);
        json = dataService.select(KEYSPACE, TABLE, selectCols, where, ConsistencyLevel.ONE);
        assertEquals("[[\"val1\",null]]", json.toString());

        // DROP TABLE
        dataService.dropTable(KEYSPACE, TABLE);
        json = dataService.select(KEYSPACE, TABLE, selectCols, where, ConsistencyLevel.ONE);
        //assertTrue("Expected exception when accessing dropped column family.", threw);

        // DROP KEYSPACE
        dataService.dropKeyspace(KEYSPACE);
        json = dataService.select(KEYSPACE, TABLE, selectCols, where, ConsistencyLevel.ONE);
    }
}