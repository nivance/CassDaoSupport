package com.joyveb.test.cassandra.domain;

import com.joyveb.support.cassandra.CassandraPrimaryKey;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class Bet extends CassandraPrimaryKey<String>{
	
    private String ticketId;
    private Integer sn;
    private String playTypeId;
    private Integer multiTimes;
    private String betNum;
    private String winLevel;
    private Integer winCount;
    private String prize;
    private Long betTax;

    public Bet(String key, String ticketId, Integer sn, String playTypeId, Integer multiTimes, String betNum, String winLevel,
    		Integer winCount, String prize, Long betTax){
    	this.key = key;
    	this.ticketId = ticketId;
    	this.sn = sn;
    	this.playTypeId = playTypeId;
    	this.multiTimes = multiTimes;
    	this.betNum = betNum;
    	this.winLevel = winLevel;
    	this.winCount = winCount;
    	this.prize = prize;
    	this.betTax = betTax;
    }

	public Bet() {
	}
    
}