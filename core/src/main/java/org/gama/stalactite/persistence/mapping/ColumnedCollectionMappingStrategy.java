package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.collection.PairIterator.EmptyIterator;
import org.gama.lang.collection.PairIterator.InfiniteIterator;
import org.gama.lang.collection.PairIterator.UntilBothIterator;
import org.gama.lang.function.Predicates;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * A class that "roughly" persists a Collection of {@link Column}s, without any bean class.
 * One may override {@link #toCollectionValue(Object)} and {@link #toDatabaseValue(Object)} to adapt values.
 * 
 * @author Guillaume Mary
 */
public class ColumnedCollectionMappingStrategy<C extends Collection<O>, O, T extends Table> implements IEmbeddedBeanMappingStrategy<C, T> {
	
	private final T targetTable;
	private final Set<Column<T, Object>> columns;
	private final ToCollectionRowTransformer<C> rowTransformer;
	private final Class<C> persistedClass;
	
	/**
	 * Constructor 
	 *
	 * @param targetTable table to persist in
	 * @param columns columns that will be used for persistent of Collections, expected to be a subset of targetTable columns    
	 * @param rowClass Class to instanciate for select from database
	 */
	public ColumnedCollectionMappingStrategy(T targetTable, Set<Column<T, Object>> columns, Class<C> rowClass) {
		this.targetTable = targetTable;
		this.columns = columns;
		this.persistedClass = rowClass;
		this.rowTransformer = new ToCollectionRowTransformer<C>(this.persistedClass) {
			
			/** We bind conversion on {@link ColumnedCollectionMappingStrategy} conversion methods */
			@Override
			protected void applyRowToBean(Row row, C collection) {
				for (Column column : getColumns()) {
					Object value = row.get(column.getName());
					collection.add(toCollectionValue(value));
				}
			}
		};
	}
	
	public Class<C> getPersistedClass() {
		return persistedClass;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	@Nonnull
	@Override
	public Set<Column<T, Object>> getColumns() {
		return columns;
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Collection<O> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new ArrayList<>();
		}
		// NB: we wrap c.iterator() in an InfiniteIterator to get all columns generated: overflow columns will have
		// null value (see 	InfiniteIterator#getValue)
		PairIterator<Column<T, Object>, O> valueColumnPairIterator = new PairIterator<>(columns.iterator(), new InfiniteIterator<>(toIterate.iterator()));
		return Iterables.map(() -> valueColumnPairIterator, Duo::getLeft, e -> toDatabaseValue(e.getRight()));
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		if (modified != null) {
			// getting differences side by side
			Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
			Iterator<O> unmodifiedIterator = unmodified == null ? new EmptyIterator<>() : unmodified.iterator();
			UntilBothIterator<? extends O, ? extends O> untilBothIterator = new UntilBothIterator<>(modified.iterator(), unmodifiedIterator);
			PairIterator<Column<T, Object>, Duo<? extends O, ? extends O>> valueColumnPairIterator = new PairIterator<>(columns.iterator(), untilBothIterator);
			valueColumnPairIterator.forEachRemaining(diffEntry -> {
				Column fieldColumn = diffEntry.getLeft();
				Duo<? extends O, ? extends O> toBeCompared = diffEntry.getRight();
				if (!Predicates.equalOrNull(toBeCompared.getLeft(), toBeCompared.getRight())) {
					toReturn.put(fieldColumn, toDatabaseValue(toBeCompared.getLeft()));
				} else {
					unmodifiedColumns.put(fieldColumn, toBeCompared.getRight());
				}
			});
			
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
			for (Column column : columns) {
				toReturn.put(column, null);
			}
		}
		
		return convertToUpwhereColumn(toReturn);
	}
	
	private Map<UpwhereColumn<T>, Object> convertToUpwhereColumn(Map<? extends Column<T, Object>, Object> map) {
		Map<UpwhereColumn<T>, Object> convertion = new HashMap<>();
		map.forEach((c, s) -> convertion.put(new UpwhereColumn<>(c, true), s));
		return convertion;
	}
	
	/**
	 * Gives the database (JDBC) value of the argument.
	 * This implementation returns the given argument without transformation.
	 * This may duplicate behavior of {@link org.gama.stalactite.sql.binder.PreparedStatementWriter} in some way, but is located to this strategy so can be
	 * more accurate.
	 * 
	 * @param object any object took from a pesistent collection
	 * @return the value to be persisted
	 */
	protected Object toDatabaseValue(O object) {
		return object;
	}
	
	/**
	 * Opposit of {@link #toDatabaseValue(Object)}: converts the database value for the collection value
	 * This implementation returns the given argument without transformation.
	 * This may duplicate behavior of {@link org.gama.stalactite.sql.binder.ResultSetReader} in some way, but is located to this strategy so can be
	 * more accurate.
	 * 
	 * @param object the value coming from the database {@link java.sql.ResultSet}
	 * @return a value for a Map
	 */
	protected O toCollectionValue(Object object) {
		return (O) object;
	}
	
	@Override
	public C transform(Row row) {
		return this.rowTransformer.transform(row);
	}
	
	@Override
	public Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		throw new UnsupportedOperationException(Reflections.toString(ColumnedCollectionMappingStrategy.class) + " can't export a mapping between some accessors and their columns");
	}
	
	@Override
	public AbstractTransformer<C> copyTransformerWithAliases(Function<Column, String> aliasProvider) {
		return new ToCollectionRowTransformer<C>(getPersistedClass()) {
			/** We bind conversion on {@link ColumnedCollectionMappingStrategy} conversion methods */
			@Override
			protected void applyRowToBean(Row row, C collection) {
				for (Column column : getColumns()) {
					Object value = row.get(aliasProvider.apply(column));
					collection.add(toCollectionValue(value));
				}
			}
		};
	}
}
