package org.gama.stalactite.persistence.sql.result;

import org.gama.stalactite.persistence.structure.Table.Column;

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
	public void put(Column column, Object object) {
		content.put(column.getAbsoluteName(), object);
	}
	
	public Object get(String key) {
		return content.get(key);
	}
	public Object get(Column key) {
		return content.get(key.getAbsoluteName());
	}
	
	public Map<String, Object> getContent() {
		return content;
	}
}
