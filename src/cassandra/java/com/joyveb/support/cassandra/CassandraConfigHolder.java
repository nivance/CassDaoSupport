package com.joyveb.support.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

public class CassandraConfigHolder extends PropertyPlaceholderConfigurer {

	private static String CASSANDRA_CONFIG_FILE = "cassandra.properties";

	public void init() {
		File file = new File(CASSANDRA_CONFIG_FILE);
		try {
			InputStream input = new FileInputStream(file);
			Resource location = new InputStreamResource(input);
			super.setLocation(location);
		} catch (IOException e) {
			System.out.println("cass file:" + file.getAbsolutePath() + "\n");
			e.printStackTrace();
		}
	}

}
