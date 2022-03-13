package org.codefilarete.stalactite.persistence.sql.ddl.structure;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.pair;

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
	
	public KeepOrderSet<Column<T, Object>> getColumns() {
		return new KeepOrderSet<>(columns.keySet().toArray(new Column[0]));
	}
	
	public String getName() {
		return name;
	}
	
	public KeepOrderSet<Column<U, Object>> getTargetColumns() {
		return new KeepOrderSet<>(columns.values().toArray(new Column[0]));
	}
	
	public T getTable() {
		return table;
	}
	
	public U getTargetTable() {
		return targetTable;
	}
}
