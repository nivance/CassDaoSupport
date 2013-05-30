package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.test.cassandra.domain.SimDrawnumber;

public interface SimDrawnumberDAO {
	
	public void insertOrUpdate(SimDrawnumber drawNumber);
	
	SimDrawnumber getDrawnumber(String playname, String termcode);
    
    SimDrawnumber getNextDrawnumber(String playname, Short state);

    SimDrawnumber selectByPrimaryKey(String _key);
    
    void updateByPrimaryKeySelective(SimDrawnumber record);

    void batchInsertSelective(final List<SimDrawnumber> list);
    
    List<SimDrawnumber> selectByCQL(String cql);
}