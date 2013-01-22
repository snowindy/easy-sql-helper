package com.github.snowindy.sql.ex;

import com.github.snowindy.sql.SQLLogUtils;

public class NoRecordReturnedException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6346578339798679598L;

	public NoRecordReturnedException(String sql, Object... params) {
		super("Query returned 0 rows: " + SQLLogUtils.getLogStr(sql, params));
	}
}
