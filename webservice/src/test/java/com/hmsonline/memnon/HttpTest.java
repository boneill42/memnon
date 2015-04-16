package com.hmsonline.memnon;

import static org.junit.Assert.assertEquals;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTest extends MemnonServerTest {
	private static final String HOST = "localhost";
	private static final String SCHEME = "http";
	private static final int PORT = 4242;
	private static final String COLUMN_FAMILY = "TEST_CF";
	private static final String KEYSPACE = "TEST_KEYSPACE";
	private static final String KEY = "TEST_ROW";

	private static Logger logger = LoggerFactory.getLogger(HttpTest.class);

	@Test
	public void testHttp() throws Exception {
		URIBuilder builder = new URIBuilder();
		CloseableHttpClient client = HttpClients.createDefault();

		// DROP KEYSPACE
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/");
		HttpDelete delete = new HttpDelete(builder.build());
		this.send(client, delete, -1);

		// CREATE KEYSPACE
		HttpPut put = new HttpPut(builder.build());
		this.send(client, put, 204);

		// CREATE COLUMN FAMILY
		builder = new URIBuilder();
		builder.setScheme(SCHEME)
				.setHost(HOST)
				.setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/")
				.setParameter("columns",
						"{\"col1\":\"varchar\", \"col2\":\"int\", \"col3\":\"varchar\"}")
				.setParameter("keys", "[\"col1\", \"col2\"]");
		put = new HttpPut(builder.build());
		this.send(client, put, 204);

		// INSERT ROW
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		put = new HttpPut(builder.build());
		put.setEntity(new StringEntity(
				"{\"ADDR1\":\"1234 Fun St.\",\"CITY\":\"Souderton.\"}",
				ContentType.create("appication/json", "UTF8")));
		this.send(client, put, 204);

		// FETCH ROW (VERIFY ROW INSERT)
		HttpGet get = new HttpGet(builder.build());
		String body = this.send(client, get, 200);
		assertEquals("{\"ADDR1\":\"1234 Fun St.\",\"CITY\":\"Souderton.\"}",
				body);
		logger.debug(body);

		// INSERT COLUMN
		builder = new URIBuilder();
		builder.setScheme(SCHEME)
				.setHost(HOST)
				.setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY + "/STATE/");
		put = new HttpPut(builder.build());
		put.setEntity(new StringEntity("CA", ContentType.create(
				"appication/json", "UTF8")));
		this.send(client, put, 204);

		// FETCH ROW (VERIFY COLUMN INSERT)
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		get = new HttpGet(builder.build());
		body = this.send(client, get, 200);
		assertEquals(
				"{\"ADDR1\":\"1235 Fun St.\",\"STATE\":\"CA\",\"COUNTY\":\"Montgomery\",\"CITY\":\"Souderton.\"}",
				body);
		logger.debug(body);

		// FETCH COLUMN
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY + "/CITY");
		get = new HttpGet(builder.build());
		body = this.send(client, get, 200);
		assertEquals("Souderton.", body);
		logger.debug(body);

		// DELETE COLUMN
		delete = new HttpDelete(builder.build());
		this.send(client, delete, 204);

		// VERIFY COLUMN DELETE
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		get = new HttpGet(builder.build());
		body = this.send(client, get, 200);
		assertEquals(
				"{\"ADDR1\":\"1235 Fun St.\",\"STATE\":\"CA\",\"COUNTY\":\"Montgomery\"}",
				body);
		logger.debug(body);

		// DELETE ROW
		delete = new HttpDelete(builder.build());
		this.send(client, delete, 200);

		// VERIFY ROW DELETE
		get = new HttpGet(builder.build());
		body = this.send(client, get, 204);
		assertEquals(null, body);
		logger.debug(body);

		// CLEANUP : DROP COLUMN FAMILY
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/" + COLUMN_FAMILY + "/");
		delete = new HttpDelete(builder.build());
		this.send(client, delete, -1);

		// CLEANUP : DROP KEYSPACE
		builder = new URIBuilder();
		builder.setScheme(SCHEME).setHost(HOST).setPort(PORT)
				.setPath(KEYSPACE + "/");
		delete = new HttpDelete(builder.build());
		this.send(client, delete, -1);
	}

	private String send(CloseableHttpClient client, HttpRequestBase request,
			int expect) throws Exception {
		CloseableHttpResponse response = client.execute(request);
		String body = null;
		try {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				body = EntityUtils.toString(entity);
				logger.debug(body);
			}
			if (expect > 0)
				assertEquals(expect, response.getStatusLine().getStatusCode());
		} finally {
			response.close();
		}
		return body;
	}
}
