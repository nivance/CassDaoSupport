package com.joyveb.test.cassandra.use;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Before;
import org.junit.Test;

import com.joyveb.support.cassandra.CassandraList;
import com.joyveb.support.cassandra.Example;
import com.joyveb.test.cassandra.dao.BetDAO;
import com.joyveb.test.cassandra.dao.impl.BetDAOImpl;
import com.joyveb.test.cassandra.domain.Bet;

/**
 * cassandra 使用 Example FindByPage使用说明：
 * <p>
 * 1、DAO接口类继承BaseCassandraDao类，参考BetDAO
 * <p>
 * 2、具体使用参考本类line53-65
 * <p>
 * 限制：此接口只能实现多个and关联查询，不支持or查询
 * 
 * 
 */
public class FindByPagesExam {

	final static String hosts = "192.168.3.143:9160";

	final static Cluster cluster = HFactory.getOrCreateCluster("Test Cluster",
			new CassandraHostConfigurator(hosts));

	private static Keyspace keyspace = null;
	private static String columnFamily = "BET";
	final static String kp_name = "gxsim";
	private BetDAO dao;

	@Before
	public void init() {
		dao = new BetDAOImpl(keyspace, columnFamily);
	}

	@Test
	public void findbypage() {
		int pageSize = 10;
		Example<String> example = new Example<String>();
		example.addEqExpress("WINCOUNT", 0).addEqExpress("BETTAX", (long) 0)
				.addEqExpress("PLAYTYPEID", "3000").addEqExpress("SN", 1);
		CassandraList<String, Bet> cassandraList = dao.findByPages(example, pageSize, null, null);
		
		cassandraList = dao.findByPages(example, pageSize, cassandraList.getStartKey(), null);
		
	}

	public static void main(String[] args) {
		FindByPagesExam useBet = new FindByPagesExam();

		ConfigurableConsistencyLevel cl = new ConfigurableConsistencyLevel();
		Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
		clmap.put(columnFamily, HConsistencyLevel.ONE);
		cl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		cl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
		cl.setReadCfConsistencyLevels(clmap);
		cl.setWriteCfConsistencyLevels(clmap);

		keyspace = HFactory.createKeyspace(kp_name, cluster, cl);

		useBet.init();
		useBet.findbypage();
	}
}
