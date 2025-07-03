package org.codefilarete.stalactite.sql.result;

import java.util.TreeMap;

/**
 * Simple ResultSet row container, with case-insensitive column name matching
 * 
 * @author Guillaume Mary
 */
public class Row extends AbstractRow<String> {
	
	public Row() {
		super(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
	}
	
	/**
	 * Overridden for accurate return type
	 * @param key the key of the value
	 * @param object the value
	 * @return this
	 */
	public Row add(String key, Object object) {
		return (Row) super.add(key, object);
	}
}
