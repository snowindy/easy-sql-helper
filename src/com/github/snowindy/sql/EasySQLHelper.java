package com.github.snowindy.sql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;

import com.github.snowindy.sql.ex.MultiResultInSingleObjectQueryException;
import com.github.snowindy.sql.ex.NoRecordReturnedException;

/**
 * Helper class to ease SQL statements execution.
 * 
 * @author esapozhnikov
 * 
 */
public class EasySQLHelper extends EasyHelperBase {

    public EasySQLHelper(Logger logger, String methodName, String jndiDS) {
        this.jndiDS = jndiDS;
        this.logger = logger;
        this.methodName = methodName;
    }

    public EasySQLHelper(Logger logger, String methodName, DataSource dataSource) {
        this.dataSource = dataSource;
        this.logger = logger;
        this.methodName = methodName;
    }

    /**
     * This constructor is equal to calling
     * {@code EasySQLHelper(Logger logger, String jndiDS)} and then
     * {@code reuseConnection()}. Which means you must use
     * {@code closeReusedResources()} in finally statement.
     * 
     * @param logger
     * @param connection
     */
    public EasySQLHelper(Logger logger, String methodName, Connection connection) {
        this.logger = logger;
        this.methodName = methodName;
        reuseConnection(connection);
    }

    public EasySQLHelper(Logger logger, String methodName, ConnInitter initter) {
        this.logger = logger;
        this.connInitter = initter;
        this.methodName = methodName;
    }

    public int[] updateBatch(String sql, List<?> list, ParamsForBatchExtractor paramExtractor) {
        lastQueryOrUpdate = sql;
        lastQueryOrUpdateParams = null;

        ResourceHolder rHldr = new ResourceHolder();
        try {
            rHldr = obtainConnection(connInitter);

            if (logger.isDebugEnabled()) {
                logger.debug("{} [Batch objects count] = [ {} ]", SQLLogUtils.getLogStr(sql, null), list.size());
            }

            rHldr.pstm = rHldr.connection.prepareStatement(sql);

            for (Object obj : list) {
                setParameters(rHldr.pstm, paramExtractor.getParams(obj));
                rHldr.pstm.addBatch();
            }

            long start = 0;
            if (logger.isDebugEnabled()) {
                start = System.currentTimeMillis();
            }

            int[] res = rHldr.pstm.executeBatch();

            if (logger.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                logger.debug("Batch update executed in {} seconds.\n[SQL Rows affected count] = {}",
                        ((end - start) / 1000.0), Arrays.asList(res));
            }

            return res;
        } catch (Exception e) {
            logger.error("\nError occured when tried to execute update: {} [Batch objects count] = [ {} ]",
                    SQLLogUtils.getLogStr(sql, null), list.size(), e);
            throw exceptionGenerator.wrap(e);
        } finally {
            rHldr.cleanUpStatementBatch();
            if (reusedConnection == null) {
                rHldr.cleanUpDatabaseConnection();
            }
        }
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
        return query(sql, EMPTY_PARAMS, rowMapper);
    }

    public void queryNoResult(String sql, Object... params) {
        query(sql, params, null);
    }

    public List<String> queryForStrings(String sql) {
        return queryForStrings(sql, EMPTY_PARAMS);
    }

    public List<String> queryForStrings(String sql, Object... params) {
        return query(sql, params, new RowMapper<String>() {
            public String mapRow(ResultSet rs, int idx) throws SQLException {
                Object o = rs.getObject(1);
                return o != null ? o.toString() : null;
            }
        });
    }

    public List<String> queryForStrings(String sql, List<?> params) {
        return queryForStrings(sql, params.toArray());
    }

    public <T> List<T> query(String sql, Object param, RowMapper<T> rowMapper) {
        return query(sql, new Object[] { param }, rowMapper);
    }

    public ResultRow queryForOneResultRow(final String sql, List<?> params) {
        return queryForOneResultRow(sql, params.toArray());
    }

    public ResultRow queryForOneResultRow(final String sql, final Object... params) {
        List<ResultRow> lst = query(sql, params, new RowMapper<ResultRow>() {
            public ResultRow mapRow(ResultSet rs, int idx) throws Exception {
                if (idx > 0) {
                    throw new MultiResultInSingleObjectQueryException(sql, params);
                }

                return new ResultRow(rs);
            }
        });
        if (lst.isEmpty()) {
            throw new NoRecordReturnedException(sql, params);
        }
        ResultRow res = lst.get(0);
        if (logger.isDebugEnabled()) {
            logger.debug("\nQuery for one row result: {}", res);
        }
        return res;
    }

    public <T> List<T> query(String sql, List<?> params, RowMapper<T> rowMapper) {
        return query(sql, params.toArray(), rowMapper);
    }

    private int lastResSetIterationCount = 0;

    public int getLastResSetIterationCount() {
        return lastResSetIterationCount;
    }

    public String getLastQueryLog() {
        return SQLLogUtils.getLogStr(lastQueryOrUpdate, lastQueryOrUpdateParams);
    }

    String lastQueryOrUpdate;

    Object[] lastQueryOrUpdateParams;

    int[] nonSevereSQLExCodesForNextReq;

    public <T> List<T> query(String sql, Object[] params, RowMapper<T> rowMapper) {

        lastQueryErrorCode = 0;

        params = unwrapParams(params);

        lastQueryOrUpdate = sql;
        lastQueryOrUpdateParams = params;

        ResourceHolder rHldr = null;
        try {
            rHldr = obtainConnection(connInitter);

            logSQL(true, sql, params);

            rHldr.pstm = rHldr.connection.prepareStatement(sql);

            setParameters(rHldr.pstm, params);

            long start = 0;
            if (logger.isDebugEnabled()) {
                start = System.currentTimeMillis();
            }

            rHldr.resSet = rHldr.pstm.executeQuery();

            if (logger.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                logger.debug("Query executed in {} seconds.", ((end - start) / 1000.0));
            }

            ResultSet rs = rHldr.resSet;

            if (rowMapper == null) {
                return Collections.emptyList();
            }
            List<T> res = new ArrayList<T>();
            int i = 0;
            try {
                while (rs.next()) {
                    T o = rowMapper.mapRow(rs, i++);
                    if (o != null) {
                        res.add(o);
                    }
                }
            } catch (Exception e) {
                // When exception occurs try to pring result set row values.
                try {
                    logger.error(SQLLogUtils.printResultSetRowContents(rs));
                } catch (Exception ex) {
                    logger.error("\nCannot create result set row detalization.", ex);
                }
                throw e;
            }

            lastResSetIterationCount = i;

            if (logger.isDebugEnabled()) {
                logger.debug("\n[SQL Query result count (RS iterations = {})] = {}", i, res.size());
            }

            return res;
        } catch (SQLException e) {
            lastQueryErrorCode = e.getErrorCode();

            boolean nonSevereSQLEx = false;

            if (nonSevereSQLExCodesForNextReq != null) {
                for (int code : nonSevereSQLExCodesForNextReq) {
                    if (lastQueryErrorCode == code) {
                        nonSevereSQLEx = true;
                        break;
                    }
                }
            }

            if (nonSevereSQLEx) {
                logger.debug("\nExpected exception occured when tried to execute query: {}. {}",
                        SQLLogUtils.getLogStr(sql, params), e.getMessage());
            } else {
                logQueryException(e, sql, params);
            }

            throw exceptionGenerator.wrap(e);

        } catch (Exception e) {
            logQueryException(e, sql, params);

            throw exceptionGenerator.wrap(e);

        } finally {
            nonSevereSQLExCodesForNextReq = null;
            if (rHldr != null) {
                rHldr.cleanUpResultSet();
                rHldr.cleanUpStatement();
                if (reusedConnection == null) {
                    rHldr.cleanUpDatabaseConnection();
                }
            }
        }
    }

    public void setNonSevereSQLExCodesForNextReq(int... nonSevereSQLExCodesForNextReq) {
        this.nonSevereSQLExCodesForNextReq = nonSevereSQLExCodesForNextReq;
    }

    private void logQueryException(Exception e, String sql, Object[] params) {
        logger.error("\nError occured when tried to execute query: {}", SQLLogUtils.getLogStr(sql, params), e);
    }

    private void setParameters(PreparedStatement pstm, Object[] params) {
        if (params != null) {
            int i = 1;
            for (Object object : params) {
                setParameter(pstm, object, i++);
            }
        }
    }

    public static boolean isCollection(Object ob) {
        return ob != null && isClassCollection(ob.getClass());
    }

    public static boolean isClassCollection(Class c) {
        return Collection.class.isAssignableFrom(c);
    }

    public int update(String sql) {
        return update(sql, (List) null);
    }

    public int update(String sql, List params) {
        return update(sql, params != null ? params.toArray() : (Object[]) null);
    }

    public int update(String sql, Object... params) {
        return update(sql, false, params);
    }

    private String generatedKey;

    public String getGeneratedKey() {
        return generatedKey;
    }

    /**
     * Use getGeneratedKey() method to obtain generated key.
     */
    public int updateWithGeneratedKey(String sql, Object... params) {
        return update(sql, true, params);
    }

    public int getLastQueryErrorCode() {
        return lastQueryErrorCode;
    }

    int lastQueryErrorCode = 0;

    public int update(String sql, boolean useGeneratedKey, Object... params) {

        lastQueryErrorCode = 0;

        params = unwrapParams(params);

        lastQueryOrUpdate = sql;
        lastQueryOrUpdateParams = params;

        ResourceHolder rHldr = null;
        try {
            rHldr = obtainConnection(connInitter);

            logSQL(true, sql, params);

            if (useGeneratedKey) {
                rHldr.pstm = rHldr.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                rHldr.pstm = rHldr.connection.prepareStatement(sql);
            }

            setParameters(rHldr.pstm, params);

            long start = 0;
            if (logger.isDebugEnabled()) {
                start = System.currentTimeMillis();
            }

            int res = 0;
            try {
                res = rHldr.pstm.executeUpdate();
            } catch (SQLException e) {
                lastQueryErrorCode = e.getErrorCode();
                throw e;
            }

            if (logger.isDebugEnabled()) {
                long end = System.currentTimeMillis();
                logger.debug("Update executed in {} seconds.\n[SQL Rows affected count] = {}",
                        ((end - start) / 1000.0), res);
            }

            if (useGeneratedKey) {
                ResultSet generatedKeys = rHldr.pstm.getGeneratedKeys();
                if (generatedKeys.next()) {
                    generatedKey = String.valueOf(generatedKeys.getObject(1));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Generated key is '{}'.", generatedKey);
                    }
                } else {
                    throw new SQLException(String.format(
                            "No generated key obtained in query. Affected rows number is %s.", res));
                }
            }

            return res;
        } catch (Exception e) {
            logger.error("\nError occured when tried to execute update: {}", SQLLogUtils.getLogStr(sql, params), e);
            throw exceptionGenerator.wrap(e);
        } finally {
            if (rHldr != null) {
                rHldr.cleanUpStatement();
                if (reusedConnection == null) {
                    rHldr.cleanUpDatabaseConnection();
                }
            }
        }
    }

    private Object[] unwrapParams(Object[] params) {
        Object[] paramsRes = null;

        if (params != null) {
            List<Object> p = new ArrayList<Object>();
            for (Object object : params) {
                if (object == null) {
                    p.add(object);
                } else if (isCollection(object)) {
                    for (Object object2 : (Collection) object) {
                        p.add(object2);
                    }
                } else if (object.getClass().isArray()) {
                    Object[] objectArr = (Object[]) object;
                    for (Object object2 : objectArr) {
                        p.add(object2);
                    }
                } else {
                    p.add(object);
                }
            }
            paramsRes = p.toArray();
        }

        return paramsRes;
    }

    /*
     * Parameter setup
     */
    private void setParameter(PreparedStatement prstmt, Object param, int index) {
        try {
            if (prstmt == null) {
                throw new IllegalArgumentException("PreparedStatement is null");
            }

            if (index < 0) {
                throw new IllegalArgumentException("Index parameter less than zero");
            }

            if (param == null) {
                prstmt.setObject(index, param);
                return;
            }

            if (paramsExtractors != null) {
                for (ParamsExtractor paramsExtractor : paramsExtractors) {
                    Object param1 = paramsExtractor.toSimpleType(param);
                    if (paramsExtractor.wasApplied()) {
                        param = param1;
                        break;
                    }
                }
            }

            if (param instanceof String) {
                prstmt.setString(index, (String) param);
            } else if (param instanceof Timestamp) {
                prstmt.setTimestamp(index, (Timestamp) param);
            } else if (param instanceof java.util.Date) {
                prstmt.setDate(index, new Date(((java.util.Date) param).getTime()));
            } else if (param instanceof Integer) {
                prstmt.setLong(index, new Long((Integer) param).longValue());
            } else if (param instanceof Long) {
                prstmt.setLong(index, ((Long) param).longValue());
            } else if (param instanceof BigDecimal) {
                prstmt.setBigDecimal(index, (BigDecimal) param);
            } else if (param.getClass().isEnum()) {
                prstmt.setString(index, String.valueOf(param));
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                            "\nParameter type is not recognized ('toString' is used): object class = {} \nobject = {}",
                            param.getClass().getName(), param);
                }
                prstmt.setString(index, param.toString());
            }
        } catch (Exception e) {
            throw exceptionGenerator.wrap(e);
        }
    }

    private static Object[] EMPTY_PARAMS = new Object[] {};

    public <T> T queryForObject(String sql, RowMapper<T> rowMapper) {
        return queryForObject(sql, EMPTY_PARAMS, rowMapper);
    }

    public <T> T queryForObject(String sql, Object param, RowMapper<T> rowMapper) {
        return queryForObject(sql, new Object[] { param }, rowMapper);
    }

    public <T> T queryForObject(String sql, Object[] params, RowMapper<T> rowMapper) {
        List<T> res = query(sql, params, rowMapper);
        if (res.isEmpty()) {
            return null;
        }
        if (disableMultiResultsInQueryFor && res.size() > 1) {
            throw new MultiResultInSingleObjectQueryException(SQLLogUtils.getLogStr(sql, params));
        }

        if (logger.isDebugEnabled()) {
            logger.debug("\n[SQL Query for <Type> result] = {}", res);
        }
        return res.get(0);
    }

    public <T> T queryForObject(String sql, List<?> params, RowMapper<T> rowMapper) {
        return queryForObject(sql, params.toArray(), rowMapper);
    }

    public String queryForString(String sql, Object... params) {
        return queryForObject(sql, params, new RowMapper<String>() {
            public String mapRow(ResultSet rs, int idx) throws SQLException {
                return rs.getString(1);
            }
        });
    }

    public String queryForString(String sql, List params) {
        return queryForString(sql, params.toArray());
    }

    public Integer queryForInt(String sql, Object... params) {
        return queryForObject(sql, params, new RowMapper<Integer>() {
            public Integer mapRow(ResultSet rs, int idx) throws SQLException {
                return rs.getInt(1);
            }
        });
    }

    public Integer queryForInt(String sql, List params) {
        return queryForInt(sql, params.toArray());
    }

    boolean disableMultiResultsInQueryFor = false;

    /**
     * if set to TRUE - Helper will throw exception if multiple rows are
     * selected in method like "queryForObject".
     */
    public void disableMultiResultsInQueryForStatements(boolean disable) {
        this.disableMultiResultsInQueryFor = disable;
    }

    public Long queryForLong(String sql, Object... params) {
        return queryForObject(sql, params, new RowMapper<Long>() {
            public Long mapRow(ResultSet rs, int idx) throws SQLException {
                return rs.getLong(1);
            }
        });
    }

    public Long queryForLong(String sql, List params) {
        return queryForLong(sql, params.toArray());
    }

    public BigDecimal queryForBigDecimal(String sql, Object... params) {
        return queryForObject(sql, params, new RowMapper<BigDecimal>() {
            public BigDecimal mapRow(ResultSet rs, int idx) throws SQLException {
                return rs.getBigDecimal(1);
            }
        });
    }

    public BigDecimal queryForBigDecimal(String sql, List params) {
        return queryForBigDecimal(sql, params.toArray());
    }
}
