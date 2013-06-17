package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.support.cassandra.BaseCassandraDao;
import com.joyveb.support.cassandra.CassandraList;
import com.joyveb.support.cassandra.Example;
import com.joyveb.support.cassandra.ExampleByCQL;
import com.joyveb.test.cassandra.domain.Bet;

public interface BetDAO extends BaseCassandraDao<String, Bet>{
	
	public List<Bet> selectByExampleWithCQL(ExampleByCQL example);
	
	public CassandraList<String, Bet> findByPages(Example<String> example, int pagesize,
			String startKey, String endKey);
}