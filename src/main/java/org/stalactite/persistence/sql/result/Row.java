package org.stalactite.persistence.sql.result;

import java.util.TreeMap;

/**
 * Simple ResultSet row container, with case-insensitive column name matching
 * 
 * @author mary
 */
public class Row {
	
	private final TreeMap<String, Object> values = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	
	public Row() {
	}
	
	public void put(String columnName, Object object) {
		values.put(columnName, object);
	}
	
	public Object get(String key) {
		return values.get(key);
	}
}
