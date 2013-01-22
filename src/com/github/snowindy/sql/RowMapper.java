package com.github.snowindy.sql;

import java.sql.ResultSet;

/**
 * Maps single result set row to return DTO type.
 * 
 * @author esapozhnikov
 * 
 * @param <T>
 */
public interface RowMapper<T> {
	public T mapRow(ResultSet rs, int idx) throws Exception;
}
