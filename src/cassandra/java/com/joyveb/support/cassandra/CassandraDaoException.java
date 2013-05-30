package com.joyveb.support.cassandra;

import org.springframework.dao.DataAccessException;

/**
 * @author limj
 * @date 2013.02.22
 *	
 *	CassandraDAOException
 */
public class CassandraDaoException extends DataAccessException {

	public CassandraDaoException(String msg) {
		super(msg);
	}

	public CassandraDaoException(String msg, Throwable cause) {
		super(msg, cause);
	}
	
	private static final long serialVersionUID = -524111705851025313L;

}
