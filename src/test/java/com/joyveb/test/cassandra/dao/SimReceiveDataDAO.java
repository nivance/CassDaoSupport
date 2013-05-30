package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.support.cassandra.BaseCassandraDao;
import com.joyveb.test.cassandra.domain.SimReceiveData;

public interface SimReceiveDataDAO extends BaseCassandraDao<String, SimReceiveData>{

    void insertOrUpdate(SimReceiveData record);

    List<SimReceiveData> checkTick(SimReceiveData example);
    
    List<SimReceiveData> getEncashData(SimReceiveData example);
     
    void batchInsertOrUpdat(final List<SimReceiveData> list);

	List<SimReceiveData> getEncashFileData(SimReceiveData dataExample);

	List<SimReceiveData> getWinDataFileData(SimReceiveData example);

}