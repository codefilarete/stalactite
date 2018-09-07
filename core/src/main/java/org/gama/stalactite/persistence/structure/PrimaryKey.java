package org.gama.stalactite.persistence.structure;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.gama.lang.collection.KeepOrderSet;

/**
 * A definition of a primary key
 * 
 * @author Guillaume Mary
 */
public class PrimaryKey<T extends Table> {
	
	private final Set<Column<T, Object>> columns = new KeepOrderSet<>();
	
	public PrimaryKey(Collection<Column<T, Object>> columns) {
		this.columns.addAll(columns);
	}
	
	public PrimaryKey(Column<T, Object> column, Column<T, Object> ... additionalColumns) {
		this.addColumns(column, additionalColumns);
	}
	
	public void addColumns(Column<T, Object> column, Column<T, Object> ... additionalColumns) {
		addColumn(column);
		for (Column<T, ?> additionalColumn : additionalColumns) {
			addColumn(additionalColumn);
		}
	}
	
	public <O extends Object> void addColumn(Column<T, O> column) {
		this.columns.add((Column<T, Object>) column);
	}
	
	public Set<Column<T, Object>> getColumns() {
		return Collections.unmodifiableSet(columns);
	}
}
