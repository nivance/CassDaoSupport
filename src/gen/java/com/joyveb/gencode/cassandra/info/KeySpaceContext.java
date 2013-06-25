package com.joyveb.gencode.cassandra.info;

import java.util.Map;

import lombok.Data;

/**
 * @author limj
 * 
 *
 */
@Data
public class KeySpaceContext {

	private String projectName;
	private String host;
	private String cluster;
	private String keyspace;
	private String targetSource;
	private String targetPackage;
	private Map<String, String> cfs;

	public KeySpaceContext(String projectName, String host, String cluster,
			String keyspace, String targetSource, String targetPackage,
			Map<String, String> cfs) {
		this.projectName = projectName;
		this.host = host;
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.targetSource = targetSource;
		this.targetPackage = targetPackage;
		this.cfs = cfs;
	}

}
