package com.github.snowindy.sql.ex;

import com.github.snowindy.sql.SQLLogUtils;

public class MultiResultInSingleObjectQueryException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7348466815883304476L;

	public MultiResultInSingleObjectQueryException(String query) {
		super("Multiple rows were fetched in single row query: " + query);
	}

	public MultiResultInSingleObjectQueryException(String sql, Object... params) {
		super("Multiple rows were fetched in single row query: " + SQLLogUtils.getLogStr(sql, params));
	}
}
