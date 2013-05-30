package com.joyveb.test.cassandra.use;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Before;
import org.junit.Test;

import com.joyveb.test.cassandra.dao.SimReceiveDataDAO;
import com.joyveb.test.cassandra.dao.impl.SimReceiveDataDAOImpl;
import com.joyveb.test.cassandra.domain.SimReceiveData;

public class RecievedDataTable {

	private CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("127.0.0.1:9160");
	private Cluster cluster = new ThriftCluster("Test Cluster", cassandraHostConfigurator);
	private Keyspace keyspace = HFactory.createKeyspace("gxtest", cluster);
	private String columnFamilyName = "T_SIM_RECEIVE_DATA";
	private SimReceiveDataDAO dao;
	
	@Before
	public void init(){
		dao = new SimReceiveDataDAOImpl(keyspace, columnFamilyName);
	}
	
	@Test
	public void insert(){
		SimReceiveData record = new SimReceiveData();
		record.setPlayname("slto");
		record.setValidtermcode("20130411");
		record.setLogiccode("1235544");
		record.setTicketcode("5");
		dao.insertOrUpdate(record);
	}
}
