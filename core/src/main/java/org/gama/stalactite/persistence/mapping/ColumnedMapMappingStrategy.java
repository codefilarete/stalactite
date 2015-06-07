package org.gama.stalactite.persistence.mapping;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.stalactite.persistence.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public abstract class ColumnedMapMappingStrategy<C extends Map<K, V>, K, V, T> implements IEmbeddedBeanMapper<C> {
	
	private final Table targetTable;
	private final Set<Column> columns;
	private final ToMapRowTransformer<C> rowTransformer;
	
	/**
	 * Constructor 
	 * 
	 * @param targetTable table to persist in
	 * @param columns columns that will be used for persistent of Maps, expected to be a subset of targetTable columns    
	 * @param rowClass Class to instanciate for select from database, expected to be C but can't be typed due to generic complexity
	 */
	public ColumnedMapMappingStrategy(@Nonnull Table targetTable, Set<Column> columns, Class<? extends Map> rowClass) {
		this.targetTable = targetTable;
		this.columns = columns;
		// weird cast cause of generics
		this.rowTransformer = new ToMapRowTransformer<C>((Class<C>) rowClass) {
			/** We bind conversion on MapMappingStrategy conversion methods */
			@Override
			protected void applyRowToBean(Row row, C map) {
				for (Column column : getColumns()) {
					String columnName = column.getName();
					K key = getKey(column);
					map.put(key, toMapValue(key, row.get(columnName)));
				}
			}
		};
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	protected String getColumnName(String columnsPrefix, int i) {
		return columnsPrefix + i;
	}
	
	@Override
	public StatementValues getInsertValues(@Nonnull C c) {
		final StatementValues toReturn = new StatementValues();
		Map<K, V> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new HashMap<>();
		}
		Iterables.visit(toIterate.entrySet(), new ForEach<Entry<K, V>, Void>() {
			@Override
			public Void visit(Entry<K, V> mapEntry) {
				Column column = getColumn(mapEntry.getKey());
				toReturn.putUpsertValue(column, toDatabaseValue(mapEntry.getKey(), mapEntry.getValue()));
				return null;
			}
		});
		// NB: on remplit les valeurs pour en avoir une par Column: celles en surplus auront une valeur null
		for (Column column : columns) {
			if (!toReturn.getUpsertValues().containsKey(column)) {
				toReturn.putUpsertValue(column, null);
			}
		}
		return toReturn;
	}
	
	@Override
	public StatementValues getUpdateValues(final C modified, C unmodified, boolean allColumns) {
		final Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
		final StatementValues toReturn = new StatementValues();
		if (modified != null) {
			// getting differences
			// - all of modified but different in unmodified
			for (Entry<K, V> modifiedEntry : modified.entrySet()) {
				K modifiedKey = modifiedEntry.getKey();
				V modifiedValue = modifiedEntry.getValue();
				Column column = getColumn(modifiedKey);
				if (!Objects.equalsWithNull(modifiedValue, unmodified == null ? null : unmodified.get(modifiedKey))) {
					toReturn.putUpsertValue(column, modifiedValue);
				} else {
					unmodifiedColumns.put(column, modifiedValue);
				}
			}
			// - all from unmodified missing in modified
			HashSet<K> missingInModified = unmodified == null ? new HashSet<K>() : new HashSet<>(unmodified.keySet());
			missingInModified.removeAll(modified.keySet());
			for (K k : missingInModified) {
				Column column = getColumn(k);
				toReturn.putUpsertValue(column, modified.get(k));
			}
			
			// adding complementary columns if necessary
			if (allColumns && !toReturn.getUpsertValues().isEmpty()) {
				Set<Column> missingColumns = new LinkedHashSet<>(columns);
				missingColumns.removeAll(toReturn.getUpsertValues().keySet());
				for (Column missingColumn : missingColumns) {
					Object missingValue = unmodifiedColumns.get(missingColumn);
					toReturn.putUpsertValue(missingColumn, missingValue);
				}
			}
		} else if (allColumns && unmodified != null) {
			for (Column column : columns) {
				toReturn.putUpsertValue(column, null);
			}
		}
		return toReturn;
	}
	
	protected abstract Column getColumn(K k);
	
	/**
	 * Expected to return the persisted value for v of key k 
	 * @param k the key being persisted, help to determine how to convert v
	 * @param v the value to be persisted
	 * @return the dabase value to be persisted
	 */
	protected abstract T toDatabaseValue(K k, V v);
	
	/**
	 * Reverse of {@link #getColumn(Object)}: give a map key from a column name
	 * @param column
	 * @return a key for a Map
	 */
	protected abstract K getKey(Column column);
	
	/**
	 * Reverse of {@link #toDatabaseValue(Object, Object)}: give a map value from a database selected value
	 * @param k the key being read, help to determine how to convert t
	 * @param t the data from the database
	 * @return a value for a Map
	 */
	protected abstract V toMapValue(K k, Object t);

	@Override
	public C transform(Row row) {
		return this.rowTransformer.transform(row);
	}
}
