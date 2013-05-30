package com.joyveb.support.cassandra;


public interface BaseCassandraDao<K, T extends CassandraPrimaryKey<K>> {
	public PageIterator<K, T> createPageIterator(K startKey, Example<K> example);
	
}
