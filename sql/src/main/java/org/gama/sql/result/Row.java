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
		// default constructor, properties are already assigned
	}
	
	/**
	 * Put a key-value pair to this instance
	 * @param columnName the key of the value
	 * @param object the value
	 */
	public void put(String columnName, Object object) {
		content.put(columnName, object);
	}
	
	/**
	 * Fluent API equivalent to {@link #put(String, Object)}
	 * @param columnName the key of the value
	 * @param object the value
	 * @return this
	 */
	public Row add(String columnName, Object object) {
		put(columnName, object);
		return this;
	}

	public Object get(String key) {
		return content.get(key);
	}

	public Map<String, Object> getContent() {
		return content;
	}
}
