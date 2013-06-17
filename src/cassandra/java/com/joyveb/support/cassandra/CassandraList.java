package com.joyveb.support.cassandra;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

/**
 * @author limj
 * @date 2013.02.18
 * 
 * @param <K>
 * @param <T>
 *            用于cassandra分页查询 K为key的类型，T为ColumnFamily的pojo类型
 */
@Data
public class CassandraList<K, T> {
	/*** 此次查询的最后一个key，下次查询从此key值开始查起 */
	private K startKey; 
	/*** 查询结果 */
	private List<T> resultList = new ArrayList<T>(); 

}
