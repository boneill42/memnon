package com.hmsonline.virgil;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class JsonMarshaller {
    private static Logger logger = LoggerFactory.getLogger(JsonMarshaller.class);

    @SuppressWarnings("unchecked")
    public static JSONArray marshallKeyspaces(List<KeyspaceMetadata> keyspaces, boolean flatten)
            throws UnsupportedEncodingException {
        JSONArray keyspaceJson = new JSONArray();
        if (flatten) {
            for (KeyspaceMetadata keyspace : keyspaces) {
                for (TableMetadata table : keyspace.getTables()) {
                    JSONObject json = new JSONObject();
                    json.put("keyspace", keyspace.getName());
                    json.put("columnFamily", table.getName());
                    keyspaceJson.add(json);
                }
            }
        } 
        return keyspaceJson;
    }
    
    @SuppressWarnings("unchecked")
    public static JSONArray marshallResultSet(ResultSet resultSet) {
        JSONArray resultJson = new JSONArray();
        for (Row row : resultSet){
            JSONArray rowJson = new JSONArray();
            resultJson.add(rowJson);
            Iterator<Definition> definitionIterator = row.getColumnDefinitions().iterator();
            while(definitionIterator.hasNext()){
                Definition definition = definitionIterator.next();
                logger.debug("Marshalling [" + definition.getName() + "] of type [" + definition.getType() + "]");
                if (definition.getType() == DataType.text()){     
                    String value = row.getString(definition.getName());
                    rowJson.add(value);
                    logger.debug("Adding [" + value + "]");
                }
            }
        }
        return resultJson;
    }
}