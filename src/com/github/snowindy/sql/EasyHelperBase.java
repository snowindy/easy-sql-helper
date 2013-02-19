package com.github.snowindy.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

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
     * You have to call {@code closeReusedResources()} in finally statement if
     * you have called {@code reuseConnection()
	 * }.
     * 
     */
    public void reuseConnection() {
        doReuseConnection = true;
    }

    protected boolean doReuseConnection;

    /**
     * You must call {@code closeReusedResources()} in finally statement if you
     * have called {@code reuseConnection() }.
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

    protected DataSource dataSource;

    protected Logger logger;

    protected String methodName;

    protected void logSQL(boolean isDebug, String sql, Object[] params) {
        if (isDebug) {
            if (logger.isDebugEnabled()) {
                logger.debug(SQLLogUtils.getLogStr(sql, params));
            }
        } else {
            logger.error(SQLLogUtils.getLogStr(sql, params));
        }
    }

    protected boolean connectionAlreadyInitted = false;

    protected ResourceHolder obtainConnection(ConnInitter initter) {
        ResourceHolder rHldr = new ResourceHolder();
        try {
            if (reusedConnection != null) {
                rHldr.connection = reusedConnection;
            } else {
                if (initter != null) {
                    if (!connectionAlreadyInitted) {
                        connectionAlreadyInitted = true;

                        rHldr.connection = initter.initConnection();
                    }
                } else if (StringUtils.isNotBlank(jndiDS)) {
                    rHldr.connection = SQLUtils.getConnection(jndiDS);
                } else if (dataSource != null) {
                    rHldr.connection = dataSource.getConnection();
                } else {
                    throw new RuntimeException("Cannot find a way to create new connection.");
                }
            }
        } catch (Exception e) {
            throw exceptionGenerator.wrap(e);
        }
        if (doReuseConnection) {
            reusedConnection = rHldr.connection;
        }

        return rHldr;
    }

    List<ParamsExtractor> paramsExtractors = null;

    public void addParamsExtractor(ParamsExtractor paramsExtractor) {
        if (paramsExtractors == null) {
            paramsExtractors = new ArrayList<ParamsExtractor>();
        }
        paramsExtractors.add(paramsExtractor);
    }

    protected ConnInitter connInitter;

    private static String COMMIT_ROLLBACK_MSG = "EasySqlHelper cannot commit or rollback in non-reuseConnection mode.";

    /**
     * Commits and closes resources.
     */
    public void commitAndClose() {
        try {
            commit();
        } finally {
            closeResources();
        }
    }

    /**
     * Commits connection, does not close resources.
     */
    public void commit() {
        if (reusedConnection == null) {
            throw new IllegalStateException(COMMIT_ROLLBACK_MSG);
        }
        try {
            reusedConnection.commit();
        } catch (Exception e) {
            logger.error("Cannot commit.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Rollbacks and closes resources.
     */
    public void rollbackAndClose() {
        try {
            rollback();
        } finally {
            closeResources();
        }
    }

    /**
     * Rollbacks connection, does not closes resources.
     */
    public void rollback() {
        if (reusedConnection == null) {
            throw new IllegalStateException(COMMIT_ROLLBACK_MSG);
        }

        SQLUtils.rollbackRE(reusedConnection);
    }
}
