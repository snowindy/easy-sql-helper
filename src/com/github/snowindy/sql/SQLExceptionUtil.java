/*
 * Created on 12.12.2008
 *
 */
package com.github.snowindy.sql;

import java.sql.SQLException;

/**
 * @author anovopashin
 * 
 */
public class SQLExceptionUtil {
	public static SQLExceptionUtil getInstance() {
		return new SQLExceptionUtil();
	}

	private SQLExceptionUtil() {
	}

	public String getAllSQLException(SQLException sqle) {
		StringBuffer exceptionBuffer = new StringBuffer(255);
		exceptionBuffer.append(sqle.getMessage());
		Exception ex = sqle.getNextException();

		while (ex != null) {
			exceptionBuffer.append("\n\tSQLException: ");
			exceptionBuffer.append(ex.getMessage());
			ex = sqle.getNextException();
		}
		return exceptionBuffer.toString();
	}

}
