/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hmsonline.memnon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.memnon.exception.MemnonException;
import com.hmsonline.memnon.resource.JsonMarshaller;

public class CassandraStorage {
	protected static Cluster cluster;
	private static Logger logger = LoggerFactory
			.getLogger(CassandraStorage.class);

	private Map<String, Session> sessions = new HashMap<String, Session>();
	private Session defaultSession = null;

	public CassandraStorage(MemnonConfiguration config) throws Exception {
		try {
			cluster = Cluster.builder()
					.addContactPoints(MemnonConfiguration.getHost()).build();
		} catch (NoHostAvailableException e) {
			throw new RuntimeException(e);
		}
	}

	public JSONArray getKeyspaces() throws MemnonException,
			UnsupportedEncodingException {
		List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
		return JsonMarshaller.marshallKeyspaces(keyspaces, true);
	}

	public void addKeyspace(String keyspace, String strategy,
			int replicationFactor) throws MemnonException {
		String statement = "create keyspace %s with replication = {'class':'%s', 'replication_factor':'%s'};";
		statement = String.format(statement, keyspace, strategy,
				replicationFactor);
		executeStatement(statement);
	}

	public void dropTable(String keyspace, String table) throws MemnonException {
		String statement = String.format("drop table %s;", table);
		executeStatement(keyspace, statement);
	}

	public void dropKeyspace(String keyspace) throws MemnonException {
		String statement = String.format("drop keyspace %s;", keyspace);
		executeStatement(statement);
	}

	// TODO: Add compactStorage flag.
	public void createTable(String keyspace, String columnFamilyName,
			JSONObject columns, JSONArray keys) throws MemnonException {
		if (columns == null || keys == null)
			throw new MemnonException(
					"Must specify columns and keys when creating a column family.");

		StringBuilder statement = new StringBuilder();
		statement.append("create table ").append(columnFamilyName).append(" (");
		statement = buildMapList(columns.entrySet().iterator(), statement);
		statement.append(", primary key (");
		statement = buildList(keys.iterator(), statement, "");
		statement.append("));");
		executeStatement(keyspace, statement.toString());
	}

	public void update(String keyspace, String table, JSONObject row,
			JSONObject where, ConsistencyLevel consistency_level)
			throws MemnonException, IOException {
		this.update(keyspace, table, row, where, consistency_level,
				System.currentTimeMillis() * 1000);
	}

	@SuppressWarnings("rawtypes")
	public void update(String keyspace, String table, JSONObject row,
			JSONObject where, ConsistencyLevel consistency_level, long timestamp)
			throws MemnonException, IOException {
		if (row == null || where == null)
			throw new MemnonException(
					"Must specify columns and values when updating a column family.");

		Update statement = QueryBuilder.update(table);

		Iterator rowIterator = row.entrySet().iterator();
		while (rowIterator.hasNext()) {
			Map.Entry pair = (Map.Entry) rowIterator.next();
			Assignment assignment = QueryBuilder.set((String) pair.getKey(),
					pair.getValue());
			statement.with(assignment);
		}

		Iterator whereIterator = where.entrySet().iterator();
		while (whereIterator.hasNext()) {
			Map.Entry pair = (Map.Entry) whereIterator.next();
			Clause clause = QueryBuilder.eq((String) pair.getKey(),
					pair.getValue());
			statement.where(clause);
		}

		executeStatement(keyspace, statement);
	}

	@SuppressWarnings("rawtypes")
	public void delete(String keyspace, String table, JSONArray columns,
			JSONObject where, ConsistencyLevel consistency_level)
			throws MemnonException, IOException {

		Delete.Selection selection = QueryBuilder.delete();
		if (columns == null) {
			selection.all();
		} else {
			Iterator columnIterator = columns.iterator();
			while (columnIterator.hasNext()) {
				selection.column((String) columnIterator.next());
			}
		}

		Delete statement = selection.from(table);
		Iterator whereIterator = where.entrySet().iterator();
		while (whereIterator.hasNext()) {
			Map.Entry pair = (Map.Entry) whereIterator.next();
			Clause clause = QueryBuilder.eq((String) pair.getKey(),
					pair.getValue());
			statement.where(clause);
		}

		executeStatement(keyspace, statement);
	}

	@SuppressWarnings("rawtypes")
	public JSONArray select(String keyspace, String table, JSONArray columns,
			JSONObject where, ConsistencyLevel consistencyLevel)
			throws MemnonException, CharacterCodingException {

		Selection selection = QueryBuilder.select();
		if (columns == null) {
			selection.all();
		} else {
			Iterator columnIterator = columns.iterator();
			while (columnIterator.hasNext()) {
				selection.column((String) columnIterator.next());
			}
		}

		Select statement = selection.from(table);
		Iterator whereIterator = where.entrySet().iterator();
		while (whereIterator.hasNext()) {
			Map.Entry pair = (Map.Entry) whereIterator.next();
			Clause clause = QueryBuilder.eq((String) pair.getKey(),
					pair.getValue());
			statement.where(clause);
		}

		ResultSet results = executeStatement(keyspace, statement);
		return JsonMarshaller.marshallResultSet(results);
	}

	/**
	 * HELPER METHODS
	 */

	private void executeStatement(String statement) {
		if (logger.isDebugEnabled())
			logger.debug("On default Session, executing [" + statement + "] ");
		getSession().execute(statement);
	}

	private void executeStatement(String keyspace, String statement) {
		if (logger.isDebugEnabled())
			logger.debug("On [" + keyspace + "], executing [" + statement
					+ "] ");
		getSession(keyspace).execute(statement);
	}

	private ResultSet executeStatement(String keyspace, Query statement) {
		if (logger.isDebugEnabled())
			logger.debug("On [" + keyspace + "], executing [" + statement
					+ "] ");
		return getSession(keyspace).execute(statement);
	}

	@SuppressWarnings("rawtypes")
	private StringBuilder buildList(Iterator iterator, StringBuilder sb,
			String delim) {
		while (iterator.hasNext()) {
			sb.append(delim).append(iterator.next()).append(delim);
			if (iterator.hasNext())
				sb.append(", ");
		}
		return sb;
	}

	@SuppressWarnings("rawtypes")
	private StringBuilder buildMapList(Iterator entries, StringBuilder sb) {
		while (entries.hasNext()) {
			Map.Entry pair = (Map.Entry) entries.next();
			sb.append(pair.getKey()).append(" ").append(pair.getValue());
			if (entries.hasNext())
				sb.append(", ");
		}
		return sb;
	}

	public synchronized Session getSession(String keyspace) {
		Session session = sessions.get(keyspace);
		if (session == null) {
			logger.debug("Constructing session for keyspace [" + keyspace + "]");
			session = cluster.connect(keyspace);
			sessions.put(keyspace, session);
		}
		return session;
	}

	public synchronized Session getSession() {
		if (defaultSession == null)
			defaultSession = cluster.connect();
		return defaultSession;
	}

}
