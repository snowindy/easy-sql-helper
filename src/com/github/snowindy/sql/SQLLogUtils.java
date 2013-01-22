package com.github.snowindy.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class SQLLogUtils {
	public static String getLogStr(String sql, Object[] params) throws RuntimeException {
		return getLogStr(sql, params, false);
	}

	public static String getLogStr(String sql, Object[] params, boolean useRetVal) throws RuntimeException {
		// trying to create 'smart' query
		StringBuilder smartQuery = null;
		try {
			if (params != null) {
				boolean paramsMatchQuestion = false;
				int numberOfQ = StringUtils.countMatches(sql, "?");
				paramsMatchQuestion = useRetVal ? numberOfQ == params.length + 1 : numberOfQ == params.length;
				if (paramsMatchQuestion) {
					smartQuery = new StringBuilder();
					String[] split = StringUtils.split(" " + sql + " ", "?");
					int slpLenMinus1 = split.length - 1;
					if (useRetVal) {
						smartQuery.append(split[0]);
						smartQuery.append("?");
					}
					for (int i = useRetVal ? 1 : 0; i < split.length; i++) {
						smartQuery.append(split[i]);
						if (i != slpLenMinus1) {
							Object param = params[i - (useRetVal ? 1 : 0)];
							formParamForLog(param, smartQuery, true);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("WRONG SMART QUERY ALGORYTHM! " + e.getMessage(), e);
		}

		if (smartQuery != null) {
			return "[Smart SQL Query] = [" + (smartQuery.toString()) + "]";
		} else {
			return "\n[SQL Query] = [" + (sql) + "]"
					+ (params != null ? "\n[Params] = [ " + formParamsForLog(params) + " ]" : "\n[SQL Params empty]");
		}
	}

	public static String getLogStr(String sql, List<? extends Object> params, boolean useRetVal)
			throws RuntimeException {
		return getLogStr(sql, params != null ? params.toArray() : null, useRetVal);
	}

	private static void formParamForLog(Object param, StringBuilder smartQuery, boolean forSmart) {
		if (param == null) {
			smartQuery.append("null");
		} else {
			String res[] = { null };
			boolean applied = forSmart && applySpecialFormatting(param, res);

			if (applied) {
				smartQuery.append(res[0]);
			} else {
				smartQuery.append("'");
				smartQuery.append(StringUtils.replace(shorten(param.toString()), "'", "''"));
				smartQuery.append("'");
			}
		}
	}

	private static int SHORTENER_BIN_LIKE_CUT_SIZE = 50;

	private static int SHORTENER_BIN_LIKE_MAX_SIZE = 400;

	private static int SHORTENER_MAX_SIZE = 1000;

	private static String shorten(String in) {
		int len = in.length();
		if (len < SHORTENER_BIN_LIKE_MAX_SIZE) {
			return in;
		} else if (len >= SHORTENER_BIN_LIKE_MAX_SIZE && !StringUtils.contains(in, " ")) {
			return in.substring(0, SHORTENER_BIN_LIKE_CUT_SIZE) + "...";
		} else if (len >= SHORTENER_MAX_SIZE) {
			return in.substring(0, SHORTENER_MAX_SIZE - 1) + "...";
		} else {
			return in;
		}
	}

	private static String formParamsForLog(Object[] params) {
		StringBuilder smartQuery = new StringBuilder();
		for (int i = 0; i < params.length; i++) {
			formParamForLog(params[i], smartQuery, false);
			if (i != params.length - 1) {
				smartQuery.append(", ");
			}

		}
		return smartQuery.toString();
	}

	private static boolean applySpecialFormatting(Object param, String[] res) {
		if (param == null) {
			return false;
		}

		if (param instanceof Timestamp) {
			SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.ms");
			res[0] = "to_timestamp('" + format.format((Date) param) + "', 'yyyymmdd hh24:MI:SS.FF')";
			return true;
		} else if (param instanceof Date) {
			SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			res[0] = "to_date('" + format.format((Date) param) + "','yyyy/mm/dd hh24:mi:ss')";
			return true;
		}

		return false;
	}

	public static String printResultSetRowContents(ResultSet rs) throws SQLException {
		ResultSetMetaData rsMetaData = rs.getMetaData();
		int columnCount = rsMetaData.getColumnCount();
		StringBuilder sb = new StringBuilder();
		sb.append("\n[Values of the row caused error while mapping]:\n");

		int maxLength = 1;
		for (int j = 1; j <= columnCount; j++) {
			if (maxLength < rsMetaData.getColumnName(j).length())
				maxLength = rsMetaData.getColumnName(j).length();
		}
		maxLength += 2;

		for (int j = 1; j <= columnCount; j++) {
			sb.append(StringUtils.rightPad(rsMetaData.getColumnName(j), maxLength));
			sb.append("->  '");
			sb.append(rs.getObject(j));
			sb.append("'\n");
		}

		return sb.toString();
	}

}
