package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;

/**
 * Index on some columns
 * 
 * @author Guillaume Mary
 */
public class Index {
	
	private final Table table;
	private final LinkedHashSet<Column> columns;
	private final String name;
	private boolean unique = false;
	
	public Index(String name, Column column, Column ... columns) {
		this(name, Collections.addAll(Arrays.asSet(column), columns));
	}
	
	public Index(String name, Iterable<Column> columns) {
		// table is took from columns
		this.table = Iterables.first(columns).getTable();
		this.columns = Iterables.copy(columns, new LinkedHashSet<>());
		this.name = name;
	}
	
	public Set<Column> getColumns() {
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
	
	public Index setUnique() {
		setUnique(true);
		return this;
	}
	
	public Table getTable() {
		return table;
	}
}
