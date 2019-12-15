package org.gama.stalactite.persistence.structure;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;

import static org.gama.lang.collection.Iterables.pair;

/**
 * Foreign key between tables
 * 
 * @author Guillaume Mary
 */
public class ForeignKey<T extends Table<T>, U extends Table<U>> {
	
	private final T table;
	private final String name;
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order 
	private final LinkedHashMap<Column<T, ?>, Column<U, ?>> columns;
	private final U targetTable;
	
	public <I> ForeignKey(String name, Column<T, I> column, Column<U, I> targetColumn) {
		this(name, Arrays.asSet(column), Arrays.asSet(targetColumn));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, LinkedHashSet<Column<T, ?>> columns, LinkedHashSet<Column<U, ?>> targetColumns) {
		this(name, pair(columns, targetColumns, LinkedHashMap::new));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, LinkedHashMap<Column<T, ?>, Column<U, ?>> columns) {
		// table is took from columns
		Entry<Column<T, ?>, Column<U, ?>> firstEntry = Iterables.first(columns.entrySet());
		this.table = firstEntry.getKey().getTable();
		this.targetTable = firstEntry.getValue().getTable();
		this.name = name;
		this.columns = columns;
	}
	
	public Set<Column<T, Object>> getColumns() {
		return (Set<Column<T, Object>>) (Set) columns.keySet();
	}
	
	public String getName() {
		return name;
	}
	
	public Collection<Column<U, Object>> getTargetColumns() {
		return (Collection<Column<U, Object>>) (Collection) columns.values();
	}
	
	public T getTable() {
		return table;
	}
	
	public U getTargetTable() {
		return targetTable;
	}
}
