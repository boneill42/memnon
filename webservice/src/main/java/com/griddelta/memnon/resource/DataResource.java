package com.griddelta.memnon.resource;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.griddelta.memnon.CassandraStorage;
import com.griddelta.memnon.MemnonConfiguration;
import com.griddelta.memnon.MemnonApplication;

@Path("/memnon/")
// TODO: Make consistency level configurable (HTTP Header?)
public class DataResource {
    private static Logger logger = LoggerFactory.getLogger(DataResource.class);
    private MemnonApplication memnon = null;
    private MemnonConfiguration config = null;
    public static final String CONSISTENCY_LEVEL_HEADER = "X-Consistency-Level";

    public DataResource(MemnonApplication memnon) {
        this.memnon = memnon;
        this.config = memnon.getConfig();
    }

    // ================================================================================================================
    // Keyspace Operations
    // ================================================================================================================
    @GET
    @Path("/schema/")
    @Produces({ "application/json" })
    public JSONArray getKeyspaces() throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("Listing keyspaces.");
        return getCassandraStorage().getKeyspaces();
    }

    @PUT
    @Path("/schema/{keyspace}")
    @Produces({ "application/json" })
    public void createKeyspace(@PathParam("keyspace") String keyspace,
            @DefaultValue("SimpleStrategy") @QueryParam("strategy") String strategy,
            @DefaultValue("1") @QueryParam("replicationFactor") int replicationFactor) throws Exception {

        if (logger.isDebugEnabled())
            logger.debug("Creating keyspace [" + keyspace + "]");
        getCassandraStorage().addKeyspace(keyspace, strategy, replicationFactor);
    }

    @DELETE
    @Path("/schema/{keyspace}")
    @Produces({ "application/json" })
    public void dropKeyspace(@PathParam("keyspace") String keyspace) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("Dropping keyspace [" + keyspace + "]");
        getCassandraStorage().dropKeyspace(keyspace);
    }

    // ================================================================================================================
    // Table Operations
    // ================================================================================================================
    @PUT
    @Path("/schema/{keyspace}/{table}")
    @Produces({ "application/json" })
    public void createTable(@PathParam("keyspace") String keyspace,
            @PathParam("table") String table, @QueryParam("columns") String columns,
            @QueryParam("keys") String keys) throws Exception {

        JSONObject columnsJson = parseJsonObject(columns);
        JSONArray keysJson = parseJsonArray(keys);
        if (logger.isDebugEnabled())
            logger.debug("Creating table [" + keyspace + "]:[" + table + "]" + ", columns : [" + columns
                    + "], keys [" + keys + "]");
        getCassandraStorage().createTable(keyspace, table, columnsJson, keysJson);
    }

    @DELETE
    @Path("/schema/{keyspace}/{table}")
    @Produces({ "application/json" })
    public void dropTable(@PathParam("keyspace") String keyspace,
            @PathParam("table") String table) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("Deleteing table [" + keyspace + "]:[" + table + "]");
        getCassandraStorage().dropTable(keyspace, table);
    }

    // ================================================================================================================
    // Data Operations
    // ================================================================================================================
    @PUT
    @Path("/data/{keyspace}/{table}")
    @Produces({ "application/json" })
    public void update(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
            @QueryParam("row") String row, @QueryParam("where") String where,
            @HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel) throws Exception {

        JSONObject rowJson = parseJsonObject(row);
        JSONObject whereJson = parseJsonObject(where);
        if (logger.isDebugEnabled())
            logger.debug("Update [" + keyspace + "]:[" + table + "]" + ", row : [" + row
                    + "], where : [" + where + "]");

        getCassandraStorage().update(keyspace, table, rowJson, whereJson,
                config.getConsistencyLevel());
    }

    @GET
    @Path("/data/{keyspace}/{table}")
    public JSONArray select(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
            @QueryParam("columns") String columns, @QueryParam("where") String where, @HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel)
            throws Exception {
        
        JSONArray columnsJson = parseJsonArray(columns);
        JSONObject whereJson = parseJsonObject(where);
        if (logger.isDebugEnabled())
            logger.debug("Select [" + keyspace + "]:[" + table + "], columns: [" + columns + "], where : [" + where + "]");

        return getCassandraStorage().select(keyspace, table, columnsJson, whereJson, config.getConsistencyLevel());
    }

    @DELETE
    @Path("/data/{keyspace}/{table}")
    @Produces({ "application/json" })
    public void delete(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
            @QueryParam("columns") String columns, @QueryParam("where") String where, @HeaderParam(CONSISTENCY_LEVEL_HEADER) String consistencyLevel)
            throws Exception {
        JSONObject whereJson = parseJsonObject(where);
        JSONArray columnsJson = parseJsonArray(columns);
        if (logger.isDebugEnabled())
            logger.debug("Deleting [" + keyspace + "]:[" + table + "], columns: [" + columns + "], where [" + where + "]");

        getCassandraStorage().delete(keyspace, table, columnsJson, whereJson,
                config.getConsistencyLevel());
    }

    // ================================================================================================================
    // Helper Methods
    // ================================================================================================================

    public CassandraStorage getCassandraStorage() {
        return this.memnon.getStorage();
    }

    private JSONArray parseJsonArray(String str) {
        JSONArray json;
        if (StringUtils.isNotBlank(str)) {
            json = (JSONArray) JSONValue.parse(str);
        } else {
            json = null;
        }
        return json;
    }

    private JSONObject parseJsonObject(String str) {
        JSONObject json;
        if (StringUtils.isNotBlank(str)) {
            json = (JSONObject) JSONValue.parse(str);
        } else {
            json = null;
        }
        return json;
    }

}
