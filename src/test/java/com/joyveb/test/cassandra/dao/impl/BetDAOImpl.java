package com.joyveb.test.cassandra.dao.impl;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;

import com.joyveb.support.cassandra.CassandraDaoSupport;
import com.joyveb.test.cassandra.dao.BetDAO;
import com.joyveb.test.cassandra.domain.Bet;

public class BetDAOImpl extends CassandraDaoSupport<String, Bet> implements
		BetDAO {

	public BetDAOImpl(Keyspace keyspace, String columnFamilyName) {
		super(keyspace, columnFamilyName, StringSerializer.get());
	}

	@Override
	protected Bet createPOJO() {
		return new Bet();
	}

}