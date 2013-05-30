package com.joyveb.test.cassandra.dao.impl;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.IndexedSlicesPredicate;
import me.prettyprint.cassandra.utils.Assert;
import me.prettyprint.hector.api.Keyspace;

import org.apache.cassandra.thrift.IndexOperator;

import com.joyveb.support.cassandra.CassandraDaoSupport;
import com.joyveb.test.cassandra.dao.SimDrawnumberDAO;
import com.joyveb.test.cassandra.domain.SimDrawnumber;

@Slf4j
public class SimDrawnumberDAOImpl extends CassandraDaoSupport<String, SimDrawnumber> implements
		SimDrawnumberDAO {

	private String splitter = "-";
	public SimDrawnumberDAOImpl(Keyspace keyspace, String columnFamilyName) {
		super(keyspace, columnFamilyName, StringSerializer.get());
	}
	
	public void insertOrUpdate(SimDrawnumber drawNumber){
		List<SimDrawnumber> drawnumbers = new ArrayList<SimDrawnumber>();
		drawnumbers.add(drawNumber);
		String key = drawNumber.getPlayname()+ splitter + drawNumber.getTermcode();
		drawNumber.setKey(key);
		super.batchInsert(drawnumbers);
//		Mutator<String> mutator = HFactory.createMutator(this.keyspace, stringSerializer);
//		prepareData(drawNumber, mutator);
//        getCFTemplate().executeBatch(mutator);
	}

//	private void prepareData(SimDrawnumber drawNumber, Mutator<String> mutator) {
//		String key = drawNumber.getPlayname() + "-" + drawNumber.getTermcode();
//		if(StringUtils.isNotBlank(drawNumber.getPlayname())){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createStringColumn("PLAYNAME", drawNumber.getPlayname()));			
//		}
//		if(StringUtils.isNotBlank(drawNumber.getTermcode())){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createStringColumn("TERMCODE", drawNumber.getTermcode()));						
//		}
//		if(drawNumber.getBegintime() != null){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("BEGINTIME", drawNumber.getBegintime(), stringSerializer, DateSerializer.get()));								
//		}
//		if(drawNumber.getEndtime() != null){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("ENDTIME", drawNumber.getEndtime(), stringSerializer, DateSerializer.get()));			
//		}
//		if(drawNumber.getState() != null){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("STATE", drawNumber.getState().intValue(), stringSerializer, IntegerSerializer.get()));			
//		}
//		if(StringUtils.isNotBlank(drawNumber.getBasecode())){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createStringColumn("BASECODE", drawNumber.getBasecode()));			
//		}
//		if(StringUtils.isNotBlank(drawNumber.getSpecialcode())){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createStringColumn("SPECIALCODE", drawNumber.getSpecialcode()));			
//		}
//		if(drawNumber.getCreatedate() != null){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("CREATEDATE", drawNumber.getCreatedate(), stringSerializer, DateSerializer.get()));			
//		}
//		if(drawNumber.getSalestatus() != null){
//			mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("SALESTATUS", drawNumber.getSalestatus().intValue(), stringSerializer, IntegerSerializer.get()));			
//		}
//        if(drawNumber.getActsellamount() != null){
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("ACTSELLAMOUNT", drawNumber.getActsellamount(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getValidsellamount() != null){
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("VALIDSELLAMOUNT", drawNumber.getValidsellamount(), stringSerializer, LongSerializer.get()));        	        	
//        }
//        if(drawNumber.getValidsellamount() != null){        	
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINTOTALAMOUNT", drawNumber.getValidsellamount(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney1() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY1", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber1() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER1", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney2() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY2", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber2() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER2", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney3() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY3", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber3() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER3", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney4() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY4", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber4() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER4", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney5() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY5", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber5() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER5", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney6() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY6", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber6() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER6", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney7() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY7", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber7() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER7", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney8() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY8", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber8() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER8", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney9() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY9", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber9() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER9", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//        if(drawNumber.getWinmoney10() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINMONEY10", drawNumber.getWinmoney1(), stringSerializer, LongSerializer.get()));        	
//        }
//        if(drawNumber.getWinnumber10() != null){   
//        	mutator.addInsertion(key, columnFamilyName, HFactory.createColumn("WINNUMBER10", drawNumber.getWinnumber1(), stringSerializer, LongSerializer.get()));
//        }
//	}

	public List<SimDrawnumber> selectByState(Short state) {
		long start = System.currentTimeMillis();
		Assert.notNull(state, "state is null");
		IndexedSlicesPredicate<String, String, Integer> predicate =  
				new IndexedSlicesPredicate<String, String, Integer>(stringSerializer, stringSerializer, IntegerSerializer.get());
		predicate.addExpression("STATE", IndexOperator.EQ, state.intValue());
		ColumnFamilyResult<String, String> result = getCFTemplate().queryColumns(predicate);
		List<SimDrawnumber> drawnumbers = cfResult2List(result);
		log.info("cost:" + (System.currentTimeMillis() - start));
		return drawnumbers;
	}

	/**
	 * This method was generated by MyBatis Generator. This method corresponds
	 * to the database table T_SIM_DRAWNUMBER
	 * 
	 * @mbggenerated Tue Aug 21 10:23:40 CST 2012
	 */
	public void updateByPrimaryKeySelective(SimDrawnumber record) {
		String key = record.getPlayname()+ splitter + record.getTermcode();
		record.setKey(key);
		super.updateByPrimaryKey(record);
		
//		String key = record.getPlayname() + "-" + record.getTermcode();
//		ColumnFamilyUpdater<String, String> updater = this.getCFTemplate().createUpdater(key);
//		if(record.getBegintime() != null){
//			updater.setDate("BEGINTIME", record.getBegintime());			
//		}
//		if(record.getEndtime() != null){
//			updater.setDate("ENDTIME", record.getEndtime());			
//		}
//		if(record.getState() != null){
//			updater.setInteger("STATE", record.getState().intValue());			
//		}
//		if(StringUtils.isNotBlank(record.getBasecode())){
//			updater.setString("BASECODE", record.getBasecode());			
//		}
//		if(StringUtils.isNotBlank(record.getSpecialcode())){
//			updater.setString("SPECIALCODE", record.getSpecialcode());			
//		}
//		if(record.getCreatedate() != null){
//			updater.setDate("CREATEDATE", record.getCreatedate());			
//		}
//		if(record.getCreatedate() != null){
//			updater.setDate("CREATEDATE", record.getCreatedate());			
//		}
//		if(record.getSalestatus() != null){
//			updater.setInteger("SALESTATUS", record.getSalestatus().intValue());			
//		}
//		if(record.getActsellamount() != null){
//			updater.setLong("ACTSELLAMOUNT", record.getActsellamount());			
//		}
//		if(record.getValidsellamount() != null){
//			updater.setLong("VALIDSELLAMOUNT", record.getValidsellamount());			
//		}
//		if(record.getWintotalamount() != null){
//			updater.setLong("WINTOTALAMOUNT", record.getWintotalamount());			
//		}
//		if(record.getWinmoney1() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber1() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney2() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber2() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney3() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber3() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney4() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber4() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney5() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber5() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney6() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber6() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney7() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber7() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney8() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber8() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney9() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber9() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//		if(record.getWinmoney10() != null){
//			updater.setLong("WINMONEY1", record.getWinmoney1());			
//		}
//		if(record.getWinnumber10() != null){
//			updater.setLong("WINNUMBER1", record.getWinnumber1());			
//		}
//  	  	this.getCFTemplate().update(updater);
	}

	public void batchInsertSelective(final List<SimDrawnumber> list) {
		for(SimDrawnumber record : list){
			String key = record.getPlayname()+ splitter + record.getTermcode();
			record.setKey(key);
		}
		super.batchInsert(list);
	}
	
	/* (non-Javadoc)
	 * @see com.joyveb.lottery.sim.cass.CassandraCFDaoSupport#cfResult2Pojo(me.prettyprint.cassandra.service.template.ColumnFamilyResult)
	 */
//	protected SimDrawnumber cfResult2Pojo(ColumnFamilyResult<String, String> result) {
//		SimDrawnumber drawNumber = new SimDrawnumber();
//		drawNumber.setPlayname(result.getString("PLAYNAME"));
//		drawNumber.setTermcode(result.getString("TERMCODE"));
//		drawNumber.setBegintime(result.getDate("BEGINTIME"));
//		drawNumber.setEndtime(result.getDate("ENDTIME"));
//		drawNumber.setState(result.getInteger("STATE"));
//		drawNumber.setBasecode(result.getString("BASECODE"));
//		drawNumber.setSpecialcode(result.getString("SPECIALCODE"));
//		drawNumber.setCreatedate(result.getDate("CREATEDATE"));
//		drawNumber.setSalestatus(result.getInteger("SALESTATUS"));
//		drawNumber.setActsellamount(result.getLong("ACTSELLAMOUNT"));
//		drawNumber.setValidsellamount(result.getLong("VALIDSELLAMOUNT"));
//		drawNumber.setWintotalamount(result.getLong("WINTOTALAMOUNT"));
//		drawNumber.setWinmoney1(result.getLong("WINMONEY1"));
//		drawNumber.setWinnumber1(result.getLong("WINNUMBER1"));
//		drawNumber.setWinmoney2(result.getLong("WINMONEY2"));
//		drawNumber.setWinnumber2(result.getLong("WINNUMBER2"));
//		drawNumber.setWinmoney3(result.getLong("WINMONEY3"));
//		drawNumber.setWinnumber3(result.getLong("WINNUMBER3"));
//		drawNumber.setWinmoney4(result.getLong("WINMONEY4"));
//		drawNumber.setWinnumber4(result.getLong("WINNUMBER4"));
//		drawNumber.setWinmoney5(result.getLong("WINMONEY5"));
//		drawNumber.setWinnumber5(result.getLong("WINNUMBER5"));
//		drawNumber.setWinmoney6(result.getLong("WINMONEY6"));
//		drawNumber.setWinnumber6(result.getLong("WINNUMBER6"));
//		drawNumber.setWinmoney7(result.getLong("WINMONEY7"));
//		drawNumber.setWinnumber7(result.getLong("WINNUMBER7"));
//		drawNumber.setWinmoney8(result.getLong("WINMONEY8"));
//		drawNumber.setWinnumber8(result.getLong("WINNUMBER8"));
//		drawNumber.setWinmoney9(result.getLong("WINMONEY9"));
//		drawNumber.setWinnumber9(result.getLong("WINNUMBER9"));
//		drawNumber.setWinmoney10(result.getLong("WINMONEY10"));
//		drawNumber.setWinnumber10(result.getLong("WINNUMBER10"));
//		return drawNumber;
//	}

//	@Override
//	protected SimDrawnumber row2Pojo(Row<String, String, String> row) {
//		SimDrawnumber drawNumber = new SimDrawnumber();
//		drawNumber.setPlayname(row.getColumnSlice().getColumnByName("PLAYNAME").getValue());
//		drawNumber.setTermcode(row.getColumnSlice().getColumnByName("TERMCODE").getValue());
//		Date date = DateSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("BEGINTIME").getValueBytes());
//		drawNumber.setBegintime(date);
//		date = DateSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("ENDTIME").getValueBytes());
//		drawNumber.setEndtime(date);
//		int state = IntegerSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("STATE").getValueBytes());
//		drawNumber.setState(state);
//		drawNumber.setBasecode(row.getColumnSlice().getColumnByName("BASECODE").getValue());
//		drawNumber.setSpecialcode(row.getColumnSlice().getColumnByName("SPECIALCODE").getValue());
//		date = DateSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("CREATEDATE").getValueBytes());
//		drawNumber.setCreatedate(date);
//		int salestatus = IntegerSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("SALESTATUS").getValueBytes());
//		drawNumber.setSalestatus(salestatus);
//		if(row.getColumnSlice().getColumnByName("ACTSELLAMOUNT") != null){
//			long amount = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("ACTSELLAMOUNT").getValueBytes());
//			drawNumber.setActsellamount(amount);
//		}
//		if(row.getColumnSlice().getColumnByName("VALIDSELLAMOUNT") != null){
//			long amount = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("VALIDSELLAMOUNT").getValueBytes());
//			drawNumber.setValidsellamount(amount);
//		}
//		if(row.getColumnSlice().getColumnByName("WINTOTALAMOUNT") != null){
//			long amount = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINTOTALAMOUNT").getValueBytes());
//			drawNumber.setWintotalamount(amount);
//		}	
//		if(row.getColumnSlice().getColumnByName("WINMONEY1") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY1").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER1") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER1").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY2") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY2").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER2") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER2").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY3") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY3").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER3") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER3").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY4") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY4").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER4") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER4").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY5") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY5").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER5") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER5").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY6") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY6").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER6") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER6").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY7") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY7").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER7") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER7").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY8") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY8").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER8") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER8").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY9") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY9").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER9") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER9").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		if(row.getColumnSlice().getColumnByName("WINMONEY10") != null){
//			long money = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINMONEY10").getValueBytes());
//			drawNumber.setWinmoney1(money);
//		}
//		if(row.getColumnSlice().getColumnByName("WINNUMBER10") != null){
//			long number = LongSerializer.get().fromByteBuffer(row.getColumnSlice().getColumnByName("WINNUMBER10").getValueBytes());
//			drawNumber.setWinnumber1(number);
//		}
//		return drawNumber;
//	}

	@Override
	public SimDrawnumber getNextDrawnumber(String playname, Short state) {
		Assert.noneNull(playname, state);
		String cql = "select * from " + this.columnFamilyName + " where	PLAYNAME = '" 
				+ playname + "'	and STATE = " + state;
		List<SimDrawnumber> list = selectByCQL(cql);
		if(list != null && list.size() > 0){
			return list.get(0);
		}else{
			return null;
		}
	}

	@Override
	public SimDrawnumber getDrawnumber(String playname, String termcode) {
		Assert.noneNull(playname, termcode);
		String cql = "select * from " + this.columnFamilyName + " where	PLAYNAME = '" 
				+ playname + "'	and TERMCODE = '" + termcode + "'";
		List<SimDrawnumber> list = selectByCQL(cql);
		if(list != null && list.size() > 0){
			return list.get(0);
		}else{
			return null;
		}
	}

	@Override
	protected SimDrawnumber createPOJO() {
		return new SimDrawnumber();
	}

	protected boolean validateKey(SimDrawnumber t) {
		Assert.notNull(t, "bean is null.");
		Assert.notNull(t.getPlayname(), "playname is null.");
		Assert.notNull(t.getTermcode(), "termcode is null.");
		return true;
	}

}