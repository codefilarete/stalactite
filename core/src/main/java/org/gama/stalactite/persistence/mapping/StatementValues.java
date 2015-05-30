package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Class for JDBC statement values storage.
 * Based on Column->values mapping as other classes, it must distinct values for the "upsert" part of the statement
 * from the "where" part of the statement since a Column can be placed in both part (for instance: update Toto set
 * version = 1 where id = 1 and version = 2)
 *
 * @author Guillaume Mary
 */
public class StatementValues {
	
	private Map<Column, Object> upsertValues;
	
	private Map<Column, Object> whereValues;
	
	public StatementValues() {
		this(new HashMap<Column, Object>(), new HashMap<Column, Object>());
	}
	
	private StatementValues(Map<Column, Object> upsertValues, Map<Column, Object> whereValues) {
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
