package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Collection;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A definition of a primary key
 * 
 * @author Guillaume Mary
 */
public class PrimaryKey<T extends Table<T>, ID> implements Key<T, ID> {
	
	private final T table;
	private final KeepOrderSet<Column<T, ?>> columns = new KeepOrderSet<>();
	
	public PrimaryKey(Collection<Column<T, Object>> columns) {
		this.table = Iterables.first(columns).getTable();
		this.columns.addAll(columns);
	}
	
	public PrimaryKey(Column<T, Object> column, Column<T, Object> ... additionalColumns) {
		this.table = column.getTable();
		this.addColumns(column, additionalColumns);
	}
	
	@Override
	public T getTable() {
		return table;
	}
	
	public void addColumns(Column<T, Object> column, Column<T, Object> ... additionalColumns) {
		addColumn(column);
		for (Column<T, ?> additionalColumn : additionalColumns) {
			addColumn(additionalColumn);
		}
	}
	
	public <O> void addColumn(Column<T, O> column) {
		this.columns.add(column);
	}
	
	@Override
	public KeepOrderSet<Column<T, ?>> getColumns() {
		return new KeepOrderSet<>(columns);
	}
	
	@Override
	public boolean isComposed() {
		return columns.size() > 1;
	}
}
