package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.pair;

/**
 * Foreign key between tables
 * 
 * @param <T> the table owning this foreign key
 * @param <U> the table referenced by this foreign key
 * @param <ID> the type of both keys
 * @author Guillaume Mary
 */
public class ForeignKey<T extends Table<T>, U extends Table<U>, ID> implements Key<T, ID> {
	
	private final T table;
	private final String name;
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep order 
	private final KeepOrderMap<Column<T, ?>, Column<U, ?>> columns;
	private final U targetTable;
	
	public <I> ForeignKey(String name, Column<T, I> column, Column<U, I> targetColumn) {
		this(name, new KeepOrderSet<>(column), new KeepOrderSet<>(targetColumn));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, KeepOrderSet<? extends Column<T, ?>> columns, KeepOrderSet<? extends Column<U, ?>> targetColumns) {
		this(name, (KeepOrderMap<? extends Column<T, ?>, ? extends Column<U, ?>>) pair(columns, targetColumns, KeepOrderMap::new));
	}
	
	@SuppressWarnings("squid:S1319")	// wanted : we want to show that we must keep set order
	public ForeignKey(String name, KeepOrderMap<? extends Column<T, ?>, ? extends Column<U, ?>> columns) {
		// table is took from columns
		Entry<? extends Column<T, ?>, ? extends Column<U, ?>> firstEntry = Iterables.first(columns.entrySet());
		this.table = firstEntry.getKey().getTable();
		this.targetTable = firstEntry.getValue().getTable();
		this.name = name;
		this.columns = (KeepOrderMap<Column<T, ?>, Column<U, ?>>) columns;
	}
	
	@Override
	public KeepOrderSet<Column<T, ?>> getColumns() {
		return new KeepOrderSet<>(columns.keySet());
	}
	
	@Override
	public boolean isComposed() {
		return columns.size() > 1;
	}
	
	public String getName() {
		return name;
	}
	
	public KeepOrderSet<Column<U, ?>> getTargetColumns() {
		return new KeepOrderSet<>(columns.values());
	}
	
	public Map<Column<T, ?>, Column<U, ?>> getColumnMapping() {
		return Collections.unmodifiableMap(columns);
	}
	
	@Override
	public T getTable() {
		return table;
	}
	
	public U getTargetTable() {
		return targetTable;
	}
	
	public Key<U, ID> toReferencedKey() {
		KeyBuilder<U, ID> keyBuilder = Key.from(targetTable);
		keyBuilder.addAllColumns(columns.values());
		return keyBuilder.build();
	}
}
