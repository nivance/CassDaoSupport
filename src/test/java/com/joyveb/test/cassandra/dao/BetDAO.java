package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.support.cassandra.BaseCassandraDao;
import com.joyveb.support.cassandra.ExampleByCQL;
import com.joyveb.test.cassandra.domain.Bet;

public interface BetDAO extends BaseCassandraDao<String, Bet>{
	
	public List<Bet> selectByExampleWithCQL(ExampleByCQL example);
	
}