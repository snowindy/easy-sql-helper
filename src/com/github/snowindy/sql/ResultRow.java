package com.github.snowindy.sql;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Contains single result set row as list, provides typed accessor method to each element. Count starts from "0", not
 * "1".
 * 
 * @author esapozhnikov
 * 
 */
public class ResultRow {
	private List<Object> rowList = new ArrayList<Object>(8);

	public ResultRow(ResultSet rs) throws SQLException {
		int count = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= count; i++) {
			rowList.add(rs.getObject(i));
		}
	}

	public ResultRow(Object[] objRow) {
		rowList = Arrays.asList(objRow);
	}

	public Object get(int colNo) {
		return rowList.get(colNo);
	}

	public String getString(int colNo) {
		Object o = get(colNo);
		if (o == null) {
			return null;
		} else {
			return String.valueOf(o);
		}
	}

	public Long getLong(int colNo) {
		Object o = get(colNo);
		if (o == null) {
			return null;
		} else if (o instanceof Number) {
			return ((Number) o).longValue();
		} else {
			return Long.parseLong(o.toString());
		}
	}

	public BigDecimal getBigDecimal(int colNo) {
		Object o = get(colNo);
		if (o == null) {
			return null;
		} else if (o instanceof BigDecimal) {
			return (BigDecimal) o;
		} else {
			return new BigDecimal(o.toString());
		}
	}

	public Date getDate(int colNo) {
		Object o = get(colNo);
		if (o == null) {
			return null;
		} else if (o instanceof Date) {
			return (Date) o;
		} else {
			throw new RuntimeException("Object from result set is not of type java.sql.Date : " + o);
		}
	}

	public String toString() {
		return "ResultRow ( " + this.rowList + " )";
	}
}
