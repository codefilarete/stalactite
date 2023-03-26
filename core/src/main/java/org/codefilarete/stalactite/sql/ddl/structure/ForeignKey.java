package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Map.Entry;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.pair;

/**
 * Foreign key between tables
 * 
 * @author Guillaume Mary
 */
public class ForeignKey<T extends Table<T>, U extends Table<U>, ID> implements Key<T, ID> {
	
	private final T table;
	private final String name;
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep order 
	private final KeepOrderMap<Column<T, Object>, Column<U, Object>> columns;
	private final U targetTable;
	
	public <I> ForeignKey(String name, Column<T, I> column, Column<U, I> targetColumn) {
		this(name, new KeepOrderSet(column), new KeepOrderSet(targetColumn));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, KeepOrderSet<? extends Column<T, Object>> columns, KeepOrderSet<? extends Column<U, Object>> targetColumns) {
		this(name, (KeepOrderMap<? extends Column<T, Object>, ? extends Column<U, Object>>) pair(columns, targetColumns, KeepOrderMap::new));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, KeepOrderMap<? extends Column<T, Object>, ? extends Column<U, Object>> columns) {
		// table is took from columns
		Entry<? extends Column<T, Object>, ? extends Column<U, Object>> firstEntry = Iterables.first(columns.entrySet());
		this.table = firstEntry.getKey().getTable();
		this.targetTable = firstEntry.getValue().getTable();
		this.name = name;
		this.columns = (KeepOrderMap<Column<T, Object>, Column<U, Object>>) columns;
	}
	
	@Override
	public KeepOrderSet<Column<T, Object>> getColumns() {
		return new KeepOrderSet<>(columns.keySet());
	}
	
	@Override
	public boolean isComposed() {
		return columns.size() > 1;
	}
	
	public String getName() {
		return name;
	}
	
	public KeepOrderSet<Column<U, Object>> getTargetColumns() {
		return new KeepOrderSet<>(columns.values());
	}
	
	@Override
	public T getTable() {
		return table;
	}
	
	public U getTargetTable() {
		return targetTable;
	}
}
