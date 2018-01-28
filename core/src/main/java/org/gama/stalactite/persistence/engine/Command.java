package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.structure.Column;

/**
 * The super type for SQL orders that write o the database : insert, update, delete.
 * Values can be set through {@link #set(Map)}, or {@link #set(Column, Object)}
 * 
 * @author Guillaume Mary
 */
public abstract class Command<SELF extends Command<SELF>> {
	
	/** The sql that contains the command */
	private final ColumnParamedSQL paramedSQL;
	
	/** SQL argument values (for where clause, or anywhere else) */
	private final List<Map<Column, Object>> values = new ArrayList<>();
	
	private Map<Column, Object> currentValues = new HashMap<>();
	
	public Command(ColumnParamedSQL paramedSQL) {
		this.paramedSQL = paramedSQL;
	}
	
	public ColumnParamedSQL getParamedSQL() {
		return paramedSQL;
	}
	
	public List<Map<Column, Object>> getValues() {
		return values;
	}
	
	public SELF set(Column column, Object value) {
		currentValues.put(column, value);
		return (SELF) this;
	}
	
	public SELF set(Map<Column, Object> values) {
		currentValues.putAll(values);
		return (SELF) this;
	}
}
