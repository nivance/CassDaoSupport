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

import com.joyveb.test.cassandra.dao.SimGameInfoDAO;
import com.joyveb.test.cassandra.dao.impl.SimGameInfoDAOImpl;

/**
 * cassandra 使用 ExampleByCQL select使用说明：
 * <p>
 * 1、、具体使用参考本类line50-64
 * <p>
 * 限制：此接口只能实现多个and关联查询，不支持or查询，暂不支持in查询
 * 
 * 
 */
public class UseCASCQL {

	//final static String hosts = "192.168.3.143:9160";
	final static String hosts = "192.168.3.222:9160";

	final static Cluster cluster = HFactory.getOrCreateCluster("Test Cluster",
			new CassandraHostConfigurator(hosts));

	private static Keyspace keyspace = null;
	private static String columnFamily = "T_SIM_GAMEINFO";
	final static String kp_name = "gxsim";
	private SimGameInfoDAO dao;

	@Before
	public void init() {
		dao = new SimGameInfoDAOImpl(keyspace, columnFamily);
	}

	public static void main(String[] args) {
		UseCASCQL cascql = new UseCASCQL();

		ConfigurableConsistencyLevel cl = new ConfigurableConsistencyLevel();
		Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
		clmap.put(columnFamily, HConsistencyLevel.ONE);
		cl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		cl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
		cl.setReadCfConsistencyLevels(clmap);
		cl.setWriteCfConsistencyLevels(clmap);

		keyspace = HFactory.createKeyspace(kp_name, cluster, cl);
		cascql.init();
		String cql = "insert into T_SIM_GAMEINFO(key, GAMENAME, LTYPE, PLAYNAME)	VALUES ( '402880d84207eae1014207eba1177c252','slto','slto', 'slto') IF NOT EXISTS";
		//insert into t_sim_gameinfo(key, "gamename","ltype", "playname") VALUES ( '402880d84207eae1014207eba1177c251','slto','slto', 'slto') IF NOT EXISTS ;
		System.out.println(cascql.dao.cas(cql));
	}
}
