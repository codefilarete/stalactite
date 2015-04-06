package org.stalactite.persistence.mapping;

import java.util.LinkedHashMap;
import java.util.Map;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class PersistentValues {
	
	private Map<Column, Object> upsertValues;
	
	private Map<Column, Object> whereValues;
	
	public PersistentValues() {
		this(new LinkedHashMap<Column, Object>(), new LinkedHashMap<Column, Object>());
	}
	
	private PersistentValues(Map<Column, Object> upsertValues, Map<Column, Object> whereValues) {
		this.upsertValues = upsertValues;
		this.whereValues = whereValues;
	}
	
	public void putUpsertValue(Column column, Object value) {
		this.upsertValues.put(column, value);
	}
	
	public void putWhereValue(Column column, Object value) {
		this.whereValues.put(column, value);
	}
	
	public Map<Column, Object> getUpsertValues() {
		return upsertValues;
	}
	
	public Map<Column, Object> getWhereValues() {
		return whereValues;
	}
}
