package org.gama.stalactite.persistence.structure;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;

import static org.gama.lang.collection.Iterables.pair;

/**
 * Foreign key between tables
 * 
 * @author Guillaume Mary
 */
public class ForeignKey {
	
	private final Table table;
	private final String name;
	private final LinkedHashMap<Column, Column> columns;
	private final Table targetTable;
	
	public ForeignKey(String name, Column column, Column targetColumn) {
		this(name, Arrays.asSet(column), Arrays.asSet(targetColumn));
	}
	
	public ForeignKey(String name, LinkedHashSet<Column> columns, LinkedHashSet<Column> targetColumns) {
		this(name, pair(columns, targetColumns, LinkedHashMap::new));
	}
	
	public ForeignKey(String name, LinkedHashMap<Column, Column> columns) {
		// table is took from columns
		this.table = Iterables.first(columns.keySet()).getTable();
		this.name = name;
		this.columns = columns;
		this.targetTable = Iterables.first(columns.values()).getTable();
	}
	
	public Set<Column> getColumns() {
		return columns.keySet();
	}
	
	public String getName() {
		return name;
	}
	
	public Collection<Column> getTargetColumns() {
		return columns.values();
	}
	
	public Table getTable() {
		return table;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
}
