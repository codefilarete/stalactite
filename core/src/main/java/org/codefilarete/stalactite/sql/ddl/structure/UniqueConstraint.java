package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Represents a unique constraint on a table.
 * 
 * @author Guillaume Mary
 */
public class UniqueConstraint<T extends Table<T>> {
	
	private final T table;
	private final KeepOrderSet<Column<T, ?>> columns;
	private final String name;
	
	public UniqueConstraint(String name, Column<T, ?> column, Column<T, ?> ... columns) {
		this(name, Collections.addAll(new KeepOrderSet<>(column), columns));
	}
	
	public UniqueConstraint(String name, Iterable<Column<T, ?>> columns) {
		// table is taken from columns
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
	
	public T getTable() {
		return table;
	}
}
