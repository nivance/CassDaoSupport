package com.joyveb.test.cassandra.use;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.Before;
import org.junit.Test;

import com.joyveb.support.cassandra.ExampleByCQL;
import com.joyveb.test.cassandra.dao.BetDAO;
import com.joyveb.test.cassandra.dao.impl.BetDAOImpl;
import com.joyveb.test.cassandra.domain.Bet;

/**
 * cassandra 使用 ExampleByCQL select使用说明：
 * <p>
 * 1、、具体使用参考本类line50-64
 * <p>
 * 限制：此接口只能实现多个and关联查询，不支持or查询，暂不支持in查询
 * 
 * 
 */
public class FindByPageUseCQL {

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
		ExampleByCQL example = new ExampleByCQL();
		example.addEqExpress("WINCOUNT", 0).addEqExpress("BETTAX", (long) 0)
				.addEqExpress("PLAYTYPEID", "3000").addEqExpress("SN", 1)
				.addEqExpress("TICKETID", "4-2012124-45873002-101852");
//		String[] ticks = new String[]{"4-2012124-45873002-101852", 
//				"4-2012124-45873002-418196", "4-2012124-45873002-306231"};
//		example.addEqExpress("WINCOUNT", 0).addEqExpress("BETTAX", (long) 0)
//				.addEqExpress("PLAYTYPEID", "3000").addEqExpress("SN", 1)
//				.addInExpress("TICKETID", ticks);
		
		System.out.println(example.toString());
		List<Bet> bets = dao.selectByExampleWithCQL(example);
		for (Bet bet : bets) {
			System.out.println(bet);
		}
	}

	public static void main(String[] args) {
		FindByPageUseCQL useBet = new FindByPageUseCQL();

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
