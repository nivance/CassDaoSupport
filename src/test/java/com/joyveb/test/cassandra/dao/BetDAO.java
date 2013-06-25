package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.support.cassandra.BaseCassandraDao;
import com.joyveb.support.cassandra.CassandraList;
import com.joyveb.support.cassandra.Example;
import com.joyveb.support.cassandra.ExampleByCQL;
import com.joyveb.test.cassandra.domain.Bet;

public interface BetDAO extends BaseCassandraDao<String, Bet> {

	public void insert(Bet t);

	public void batchInsert(List<Bet> list);

	public void updateByPrimaryKey(Bet t);
	
	public void batchUpdate(List<Bet> list);
	
	public void delete(String key);

	public Bet selectByPrimaryKey(String _key);

	public List<Bet> selectByCQL(String cql);

	public List<Bet> selectByExample(Example<String> example);
	
	public List<Bet> selectByExampleWithCQL(ExampleByCQL example);
	
	public CassandraList<String, Bet> findByPages(Example<String> example,
			int pagesize, String startKey, String endKey);

}