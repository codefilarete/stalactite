package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Index on some columns
 * 
 * @author Guillaume Mary
 */
public class Index<T extends Table<T>> {
	
	private final T table;
	private final KeepOrderSet<Column<T, ?>> columns;
	private final String name;
	private boolean unique = false;
	
	public Index(String name, Column<T, ?> column, Column<T, ?> ... columns) {
		this(name, Collections.addAll(Arrays.asSet(column), columns));
	}
	
	public Index(String name, Iterable<? extends Column<T, ?>> columns) {
		// table is took from columns
		this.table = Iterables.first(columns).getTable();
		this.columns = Iterables.copy(columns, new KeepOrderSet<>());
		this.name = name;
	}
	
	public Set<Column<T, ?>> getColumns() {
		return columns;
	}
	
	public String getName() {
		return name;
	}
	
	public boolean isUnique() {
		return unique;
	}
	
	public void setUnique(boolean unique) {
		this.unique = unique;
	}
	
	public Index<T> setUnique() {
		setUnique(true);
		return this;
	}
	
	public T getTable() {
		return table;
	}
}
