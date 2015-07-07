package org.gama.sql.result;

import java.util.Map;
import java.util.TreeMap;

/**
 * Simple ResultSet row container, with case-insensitive column name matching
 * 
 * @author Guillaume Mary
 */
public class Row {
	
	private final TreeMap<String, Object> content = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	
	public Row() {
	}
	
	public void put(String columnName, Object object) {
		content.put(columnName, object);
	}

	public Object get(String key) {
		return content.get(key);
	}

	public Map<String, Object> getContent() {
		return content;
	}
}
