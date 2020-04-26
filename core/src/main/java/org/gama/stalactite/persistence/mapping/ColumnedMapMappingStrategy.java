package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Collections;
import org.gama.lang.function.Predicates;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * A class that "roughly" persists a Map of {@link Column}s, without any bean class.
 *
 * @author Guillaume Mary
 */
public abstract class ColumnedMapMappingStrategy<C extends Map<K, V>, K, V, T extends Table> implements IEmbeddedBeanMappingStrategy<C, T> {
	
	private final T targetTable;
	private final Set<Column<T, Object>> columns;
	private final ToMapRowTransformer<C> rowTransformer;
	
	/**
	 * Constructor 
	 * 
	 * @param targetTable table to persist in
	 * @param columns columns that will be used for persistent of Maps, expected to be a subset of targetTable columns    
	 * @param rowClass Class to instanciate for select from database
	 */
	public ColumnedMapMappingStrategy(T targetTable, Set<Column<T, Object>> columns, Class<C> rowClass) {
		this.targetTable = targetTable;
		this.columns = columns;
		// We bind conversion on MapMappingStrategy conversion methods */
		this.rowTransformer = new LocalToMapRowTransformer<C, K, V>(rowClass, (Set) getColumns(), this::getKey, this::toMapValue);
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	@Nonnull
	@Override
	public Set<Column<T, Object>> getColumns() {
		return columns;
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint accessor) {
		// this class doesn't support bean factory so it can't support properties set by constructor
	}
	
	protected String getColumnName(String columnsPrefix, int i) {
		return columnsPrefix + i;
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		Map<K, V> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new HashMap<>();
		}
		toIterate.forEach((key, value) -> addUpsertValues(key, value, toReturn));
		// NB: we must return all columns: we complete non-valued columns with null 
		for (Column<T, Object> column : columns) {
			if (!toReturn.containsKey(column)) {
				toReturn.put(column, null);
			}
		}
		return toReturn;
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, Object>, Object> unmodifiedColumns = new HashMap<>();
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		if (modified != null) {
			// getting differences
			// - all of modified but different in unmodified
			for (Entry<K, V> modifiedEntry : modified.entrySet()) {
				K modifiedKey = modifiedEntry.getKey();
				V modifiedValue = modifiedEntry.getValue();
				Column<T, Object> column = getColumn(modifiedKey);
				if (!Predicates.equalOrNull(modifiedValue, unmodified == null ? null : unmodified.get(modifiedKey))) {
					toReturn.put(column, modifiedValue);
				} else {
					unmodifiedColumns.put(column, modifiedValue);
				}
			}
			// - all from unmodified missing in modified
			HashSet<K> missingInModified = unmodified == null ? new HashSet<>() : new HashSet<>(unmodified.keySet());
			missingInModified.removeAll(modified.keySet());
			for (K k : missingInModified) {
				addUpsertValues(k, modified.get(k), toReturn);
			}
			
			// adding complementary columns if necessary
			if (allColumns && !toReturn.isEmpty()) {
				Set<Column<T, Object>> missingColumns = new LinkedHashSet<>(columns);
				missingColumns.removeAll(toReturn.keySet());
				for (Column<T, Object> missingColumn : missingColumns) {
					Object missingValue = unmodifiedColumns.get(missingColumn);
					toReturn.put(missingColumn, missingValue);
				}
			}
		} else if (allColumns && unmodified != null) {
			for (Column<T, Object> column : columns) {
				toReturn.put(column, null);
			}
		}
		return convertToUpwhereColumn(toReturn);
	}
	
	private Map<UpwhereColumn<T>, Object> convertToUpwhereColumn(Map<Column<T, Object>, Object> map) {
		Map<UpwhereColumn<T>, Object> convertion = new HashMap<>();
		map.forEach((c, s) -> convertion.put(new UpwhereColumn<>(c, true), s));
		return convertion;
	}
	
	/**
	 * Add values to valuesToBePersisted according to key and value.
	 * Calls {@link #toDatabaseValue(Object, Object)} to transform value to the persisted Object
	 * 
	 * @param key the key to be persisted
	 * @param value the value ok key in the Map, may be transformed to be persisted
	 * @param valuesToBePersisted Map to populate
	 */
	protected void addUpsertValues(K key, V value, Map<Column<T, Object>, Object> valuesToBePersisted) {
		Object o = toDatabaseValue(key, value);
		Column<T, Object> column = getColumn(key);
		valuesToBePersisted.put(column, o);
	}
	
	protected abstract Column<T, Object> getColumn(K k);
	
	/**
	 * Expected to return the persisted value for v of key k 
	 * @param k the key being persisted, help to determine how to convert v
	 * @param v the value to be persisted
	 * @return the dabase value to be persisted
	 */
	protected abstract Object toDatabaseValue(K k, V v);
	
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
	
	@Override
	public AbstractTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return this.rowTransformer.copyWithAliases(columnedRow);
	}
	
	@Override
	public Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		throw new UnsupportedOperationException(Reflections.toString(ColumnedMapMappingStrategy.class) + " can't export a mapping between some accessors and their columns");
	}
	
	private static class LocalToMapRowTransformer<M extends Map<K, V>, K, V> extends ToMapRowTransformer<M> {
		
		private final Iterable<Column> columns;
		private final Function<Column, K> keyProvider;
		private final BiFunction<K /* key */, Object /* row value */, V> databaseValueConverter;
		
		private LocalToMapRowTransformer(Class<M> persistedClass,
										 Iterable<Column> columns,
										 Function<Column, K> keyProvider,
										 BiFunction<K /* key */, Object /* row value */, V> databaseValueConverter) {
			super(persistedClass);
			this.columns = columns;
			this.keyProvider = keyProvider;
			this.databaseValueConverter = databaseValueConverter;
		}
		
		private LocalToMapRowTransformer(Function<Function<Column, Object>, M> beanFactory,
										 ColumnedRow columnedRow,
										 Iterable<Column> columns, Function<Column, K> keyProvider,
										 BiFunction<K /* key */, Object /* row value */, V> databaseValueConverter) {
			super(beanFactory, columnedRow);
			this.columns = columns;
			this.keyProvider = keyProvider;
			this.databaseValueConverter = databaseValueConverter;
		}
		
		@Override
		public AbstractTransformer<M> copyWithAliases(ColumnedRow columnedRow) {
			return new LocalToMapRowTransformer<>(this.beanFactory, columnedRow, this.columns, this.keyProvider, this.databaseValueConverter);
		}
		
		/** We bind conversion on {@link ColumnedCollectionMappingStrategy} conversion methods */
		@Override
		public void applyRowToBean(Row row, M map) {
			for (Column column : this.columns) {
				K key = keyProvider.apply(column);
				V value = (V) getColumnedRow().getValue(column, row);
				map.put(key, databaseValueConverter.apply(key, value));
			}
		}
	}
}
