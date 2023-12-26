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
public class Index {
	
	private final Table table;
	private final KeepOrderSet<Column<Table, Object>> columns;
	private final String name;
	private boolean unique = false;
	
	public <T extends Table> Index(String name, Column<T, ?> column, Column<T, ?> ... columns) {
		this(name, (Iterable) Collections.addAll(Arrays.asSet(column), columns));
	}
	
	public <T extends Table> Index(String name, Iterable<Column<T, Object>> columns) {
		// table is took from columns
		this.table = Iterables.first(columns).getTable();
		this.columns = (KeepOrderSet) Iterables.copy(columns, new KeepOrderSet<>());
		this.name = name;
	}
	
	public <T extends Table> Set<Column<T, Object>> getColumns() {
		return (Set) columns;
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
	
	public Index setUnique() {
		setUnique(true);
		return this;
	}
	
	public Table getTable() {
		return table;
	}
}
