package com.griddelta.memnon.health;

import org.json.simple.JSONArray;

import com.codahale.metrics.health.HealthCheck;
import com.griddelta.memnon.MemnonApplication;
import com.griddelta.memnon.MemnonConfiguration;

public class CassandraHealthCheck extends HealthCheck {
	private MemnonApplication service;
    private MemnonConfiguration configuration;

	public CassandraHealthCheck(MemnonApplication service, MemnonConfiguration configuration) {
		this.service = service;
        this.configuration = configuration;
	}

	@Override
	public Result check() throws Exception {
		Result result = null;
		
		try {
			String host = configuration.getHost();
            int port = configuration.getPort();
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
