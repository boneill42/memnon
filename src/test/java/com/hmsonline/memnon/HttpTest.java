package com.hmsonline.memnon;

import static org.junit.Assert.assertEquals;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTest extends MemnonServerTest {
	private static final String BASE_URL = "http://localhost:8080/virgil/data/";
	private static final String COLUMN_FAMILY = "TEST_CF";
	private static final String KEYSPACE = "TEST_KEYSPACE";
	private static final String KEY = "TEST_ROW";

	private static Logger logger = LoggerFactory.getLogger(HttpTest.class);
	
	@Test
	public void testHttp() throws Exception {
		CloseableHttpClient client = HttpClients.createDefault();

		// DROP KEYSPACE
		HttpDelete delete = new HttpDelete(BASE_URL + KEYSPACE + "/");
		this.send(client, delete, -1);

		// CREATE KEYSPACE
		HttpPut put = new HttpPut(BASE_URL + KEYSPACE + "/");
		this.send(client, put, 204);

		// CREATE COLUMN FAMILY
		put = new HttpPut(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/");
		this.send(client, put, 204);

		// INSERT ROW
		put = new HttpPut(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		put.setEntity(new StringEntity("{\"ADDR1\":\"1234 Fun St.\",\"CITY\":\"Souderton.\"}",
				ContentType.create("appication/json", "UTF8")));
		this.send(client, put, 204);

		// FETCH ROW (VERIFY ROW INSERT)
		HttpGet get = new HttpGet(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		String body = this.send(client, get, 200);
		assertEquals("{\"ADDR1\":\"1234 Fun St.\",\"CITY\":\"Souderton.\"}", body);
		logger.debug(body);
				
		// INSERT COLUMN
		put = new HttpPut(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY + "/STATE/");
		put.setEntity(new StringEntity("CA", ContentType.create("appication/json", "UTF8")));
		this.send(client, put, 204);

		// FETCH ROW (VERIFY COLUMN INSERT)
		get = new HttpGet(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		body = this.send(client, get, 200);
		assertEquals("{\"ADDR1\":\"1235 Fun St.\",\"STATE\":\"CA\",\"COUNTY\":\"Montgomery\",\"CITY\":\"Souderton.\"}", body);
		logger.debug(body);

		// FETCH COLUMN
		get = new HttpGet(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY + "/CITY");
		body = this.send(client, get, 200);
		assertEquals("Souderton.", body);
		logger.debug(body);

		// DELETE COLUMN
		delete = new HttpDelete(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY + "/CITY");
		this.send(client, delete, 204);

		// VERIFY COLUMN DELETE
		get = new HttpGet(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		body = this.send(client, get, 200);
		assertEquals("{\"ADDR1\":\"1235 Fun St.\",\"STATE\":\"CA\",\"COUNTY\":\"Montgomery\"}", body);
		logger.debug(body);

		// DELETE ROW
		delete = new HttpDelete(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		this.send(client, delete, 200);

		// VERIFY ROW DELETE
		get = new HttpGet(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/" + KEY);
		body = this.send(client, get, 204);
		assertEquals(null, body);
		logger.debug(body);

		// CLEANUP : DROP COLUMN FAMILY
		delete = new HttpDelete(BASE_URL + KEYSPACE + "/" + COLUMN_FAMILY + "/");
		this.send(client, delete, -1);

		// CLEANUP : DROP KEYSPACE
		delete = new HttpDelete(BASE_URL + KEYSPACE + "/");
		this.send(client, delete, -1);
	}

	private String send(CloseableHttpClient client, HttpRequestBase request, int expect) throws Exception {
		CloseableHttpResponse response = client.execute(request);
		String body = null;
		try {
			HttpEntity entity = response.getEntity();
			body = EntityUtils.toString(entity);
			logger.debug(body);
			if (expect > 0)
				assertEquals(expect, body);			
		} finally {
			response.close();
		}
		return body;
	}
}
