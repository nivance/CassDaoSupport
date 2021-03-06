package com.joyveb.test.cassandra.dao.impl;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.Assert;
import me.prettyprint.hector.api.Keyspace;

import com.joyveb.support.cassandra.CassandraDaoSupport;
import com.joyveb.test.cassandra.dao.SimReceiveDataDAO;
import com.joyveb.test.cassandra.domain.SimReceiveData;

@Slf4j
public class SimReceiveDataDAOImpl extends CassandraDaoSupport<String, SimReceiveData> implements SimReceiveDataDAO {

	private String splitter = "-";
    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table T_SIM_RECEIVE_DATA
     *
     * @mbggenerated Tue Aug 21 10:23:40 CST 2012
     */
    public SimReceiveDataDAOImpl(Keyspace keyspace, String columnFamilyName) {
        super(keyspace, columnFamilyName, StringSerializer.get());
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table T_SIM_RECEIVE_DATA
     *
     * @mbggenerated Tue Aug 21 10:23:40 CST 2012
     */
    public void insertOrUpdate(SimReceiveData record) {
		String key = record.getPlayname() + splitter
				+ record.getValidtermcode() + splitter + record.getLogiccode()
				+ splitter + record.getTicketcode();
		record.setKey(key);
    	super.insert(record);
    }

//    public CassandraList<String, SimReceiveData> findByPage(SimReceiveData example, int pagesize, String startKey, String endKey) {
//    	RangeSlicesQuery<String, String, String> rangeSlicesQuery =  
//				HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
//    	rangeSlicesQuery.addEqualsExpression("PLAYNAME", example.getPlayname());
//    	rangeSlicesQuery.addEqualsExpression("VALIDTERMCODE", example.getValidtermcode());
//    	if(StringUtils.isNotBlank(example.getWithdraw())){
//    		rangeSlicesQuery.addEqualsExpression("WITHDRAW", example.getWithdraw());
//    		rangeSlicesQuery.setColumnNames("PLAYNAME", "VALIDTERMCODE", "WITHDRAW");
//    	}else{
//    		rangeSlicesQuery.setColumnNames("PLAYNAME", "VALIDTERMCODE");
//    	}
//        rangeSlicesQuery.setColumnFamily(columnFamilyName);
//        rangeSlicesQuery.setKeys(startKey, endKey);
//        //reason from [https://github.com/hector-client/hector/wiki/User-Guide]
//        rangeSlicesQuery.setRowCount(pagesize + 1);
//        rangeSlicesQuery.setReturnKeysOnly();
//        rangeSlicesQuery.setRange("", "", false, Integer.MAX_VALUE);
//        QueryResult<OrderedRows<String, String, String>> resultQuery = rangeSlicesQuery.execute();
//        if(resultQuery.get() != null){
//        	List<Row<String, String, String>> rows = resultQuery.get().getList();
//        	CassandraList<String, SimReceiveData> cassandraList = new CassandraList<String, SimReceiveData>();
//        	String lastKey = resultQuery.get().peekLast().getKey();
//        	cassandraList.setStartKey(lastKey);        	
//        	List<String> keys = new ArrayList<String>();
//        	for(Row<String, String, String> row : rows){
//        		keys.add(row.getKey());
//        	}
//        	if(keys.size() > pagesize){
//        		keys.remove(lastKey);        		
//        	}else{//最后一批数据,不再查询
//        		cassandraList.setStop(true);
//        	}
//        	log.info("keys:" + keys);
//        	ColumnFamilyResult<String, String> result = getCFTemplate().queryColumns(keys);   
//        	cassandraList.setResultList(cfResult2List(result));
//        	return cassandraList;   
//        }else{
//        	return null;
//        }
//    }

    /**
    * This method was generated by MyBatis Generator.
    * This method corresponds to the database table T_SIM_RECEIVE_DATA
    *
    * @mbggenerated Tue Aug 34 10:23:40 CST 2012
    */
    public void batchInsertOrUpdat(final List<SimReceiveData> list) {
    	for(SimReceiveData record : list){
    		String key = record.getPlayname() + splitter
    				+ record.getValidtermcode() + splitter + record.getLogiccode()
    				+ splitter + record.getTicketcode();
    		record.setKey(key);
    	}
    	super.batchInsert(list);
    }
    
	@Override
	public List<SimReceiveData> checkTick(SimReceiveData example) {
		Assert.noneNull(example, example.getPlayname(), example.getValidtermcode(), 
				example.getLogiccode(), example.getRuncode());
		String cql =  "select * from " + this.columnFamilyName + " where PLAYNAME = '"
				+ example.getPlayname() + "' and VALIDTERMCODE = '" 
				+ example.getValidtermcode() + "' and LOGICCODE = '" 
				+ example.getLogiccode() + "' and RUNCODE = '" + example.getRuncode() + "'";
		return selectByCQL(cql);
	}
	
	@Override
	public List<SimReceiveData> getEncashData(SimReceiveData example) {
		Assert.noneNull(example, example.getPlayname(),	example.getLogiccode(), example.getTicketcode());
		String cql =  "select * from " + this.columnFamilyName + " where PLAYNAME = '"
				+ example.getPlayname() + "' and LOGICCODE = '" 
				+ example.getLogiccode() + "' and TICKETCODE = '" + example.getTicketcode() + "'";
		log.info(this.columnFamilyName + ":cql[" + cql + "]"); 
		return selectByCQL(cql);
	}

	public List<SimReceiveData> getEncashFileData(SimReceiveData example) {
		Assert.noneNull(example, example.getPlayname(),	example.getValidtermcode(), example.getEncashflag());
		String cql =  "select * from " + this.columnFamilyName + " where PLAYNAME = '"
				+ example.getPlayname() + "' and VALIDTERMCODE = '" 
				+ example.getValidtermcode() + "' and ENCASHFLAG = " + example.getEncashflag();
		log.info(this.columnFamilyName + ":cql[" + cql + "]"); 
		return selectByCQL(cql);
	}
	
	public List<SimReceiveData> getWinDataFileData(SimReceiveData example) {
		Assert.noneNull(example, example.getPlayname(),	example.getValidtermcode(), example.getIswin());
		String cql =  "select * from " + this.columnFamilyName + " where PLAYNAME = '"
				+ example.getPlayname() + "' and VALIDTERMCODE = '" 
				+ example.getValidtermcode() + "' and ISWIN = " + example.getIswin();
		log.info(this.columnFamilyName + ":cql[" + cql + "]"); 
		return selectByCQL(cql);
	}

	@Override
	protected SimReceiveData createPOJO() {
		return new SimReceiveData();
	}

	protected boolean validateKey(SimReceiveData t) {
		Assert.notNull(t, "bean is null.");
		Assert.notNull(t.getPlayname(), "playname is null.");
		Assert.notNull(t.getValidtermcode(), "termcode is null.");
		Assert.notNull(t.getLogiccode(), "logiccode is null.");
		Assert.notNull(t.getTicketcode(), "ticketcode is null.");
		return true;
	}
	
}