package com.hmsonline.memnon.health;

import org.json.simple.JSONArray;

import com.hmsonline.memnon.MemnonService;
import com.hmsonline.memnon.MemnonConfiguration;
import com.yammer.metrics.core.HealthCheck;

public class CassandraHealthCheck extends HealthCheck {
	private MemnonService service;

	public CassandraHealthCheck(MemnonService service) {
		super("Cassandra Check");
		this.service = service;
	}

	@Override
	public Result check() throws Exception {
		Result result = null;
		
		try {
			String host = MemnonConfiguration.getHost();
            int port = MemnonConfiguration.getPort();
			JSONArray keyspaces = this.service.getStorage().getKeyspaces();
			String output = "Connected to [" + host + ":" + port + "] w/ " + keyspaces.size() + " keyspaces.";
			result = Result.healthy(output);
		} catch (Throwable e) {
			result = Result.unhealthy("Unable to connect to cluster: "
					+ e.getMessage());
		}
		return result;
	}
}
