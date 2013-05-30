package com.joyveb.test.cassandra.dao;

import java.util.List;

import com.joyveb.support.cassandra.Example;
import com.joyveb.test.cassandra.domain.SimProperties;

public interface SimPropertiesDAO {

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table T_SIM_PROPERTIES
     *
     * @mbggenerated Tue Aug 21 10:23:40 CST 2012
     */
    void insert(SimProperties record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table T_SIM_PROPERTIES
     *
     * @mbggenerated Tue Aug 21 10:23:40 CST 2012
     */
    List<SimProperties> selectByExample(Example<String> example);

}