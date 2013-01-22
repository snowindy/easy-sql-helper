package com.github.snowindy.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class EasyHelperBase {
	protected class ResourceHolder {
		Connection connection;

		PreparedStatement pstm;

		ResultSet resSet;

		protected void cleanUpDatabaseConnection() {
			EasyHelperBase.this.cleanUpDatabaseConnection(connection);
		}

		protected void cleanUpResultSet() {
			try {
				SQLUtils.cleanUpResultSet(resSet);
			} catch (Exception e) {
			}
		}

		protected void cleanUpStatement() {
			try {
				SQLUtils.cleanUpStatement(pstm);
			} catch (Exception e) {
			}
		}

		public void cleanUpStatementBatch() {
			try {
				SQLUtils.cleanUpStatementBatch(pstm);
			} catch (Exception e) {
			}
		}
	}

	protected void cleanUpDatabaseConnection(Connection connection) {
		try {
			SQLUtils.cleanUpDatabaseConnection(connection);
		} catch (Exception e) {
			throw exceptionGenerator.wrap(e);
		}
	}

	protected ExceptionGenerator exceptionGenerator = defaultExceptionGenerator;

	static ExceptionGenerator defaultExceptionGenerator = new ExceptionGenerator() {

		public RuntimeException wrap(Exception e) throws RuntimeException {
			throw new RuntimeException(e);
		}
	};

	protected void reuseConnection(Connection connection) {
		reusedConnection = connection;
		reuseConnection();
	}

	/**
	 * You have to call {@code closeReusedResources()} in finally statement if you have called {@code reuseConnection()
	 * }.
	 * 
	 */
	public void reuseConnection() {
		doReuseConnection = true;
	}

	protected boolean doReuseConnection;

	/**
	 * You must call {@code closeReusedResources()} in finally statement if you have called {@code reuseConnection() }.
	 * 
	 * @throws SQLException
	 * 
	 */
	public void closeResources() {
		try {
			if (reusedConnection != null) {
				cleanUpDatabaseConnection(reusedConnection);
			}
		} catch (Exception e) {
			throw exceptionGenerator.wrap(e);
		}
	}

	protected Connection reusedConnection;

	protected String jndiDS;

	protected Logger logger;

	protected String className;

	protected String methodName;

	protected void logSQL(Level lev, String sql, Object[] params) {
		if (logger.isLoggable(lev)) {
			logger.logp(lev, className, methodName, SQLLogUtils.getLogStr(sql, params));
		}
	}

	protected boolean connectionAlreadyInitted = false;

	protected ResourceHolder obtainConnection(ConnInitter initter) {
		ResourceHolder rHldr = new ResourceHolder();
		try {
			if (reusedConnection != null) {
				resHolders.add(rHldr);
				rHldr.connection = reusedConnection;
			} else {
				if (initter != null) {
					if (!connectionAlreadyInitted) {
						connectionAlreadyInitted = true;

						rHldr.connection = initter.initConnection();
						if (doReuseConnection) {
							reusedConnection = rHldr.connection;
						}
					}
				} else {
					rHldr.connection = SQLUtils.getConnection(jndiDS);
				}
			}
		} catch (Exception e) {
			throw exceptionGenerator.wrap(e);
		}
		return rHldr;
	}

	protected List<ResourceHolder> resHolders = new LinkedList<ResourceHolder>();

	List<ParamsExtractor> paramsExtractors = null;

	public void addParamsExtractor(ParamsExtractor paramsExtractor) {
		if (paramsExtractors == null) {
			paramsExtractors = new LinkedList<ParamsExtractor>();
		}
		paramsExtractors.add(paramsExtractor);
	}

	protected ConnInitter connInitter;
}
