package org.codefilarete.stalactite.sql.order;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Placeholder for a named variable in an update statement, used in the update where clause (criteria part)
 * 
 * @param <V>
 * @param <T>
 * @author Guillaume Mary
 */
public class PlaceholderVariable<V, T extends Table<T>> extends StatementVariable<V, T> {
	
	private final String name;
	private final V value;
	
	public PlaceholderVariable(String name, V value) {
		this.name = name;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}
	
	public V getValue() {
		return value;
	}
	
	public void applyValueTo(WherableStatement updateStatement) {
		updateStatement.setValue(name, value);
	}
}