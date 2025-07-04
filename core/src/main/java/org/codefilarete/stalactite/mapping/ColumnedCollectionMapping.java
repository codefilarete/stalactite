package org.codefilarete.stalactite.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.tool.collection.PairIterator.EmptyIterator;
import org.codefilarete.tool.collection.PairIterator.InfiniteIterator;
import org.codefilarete.tool.collection.PairIterator.UntilBothIterator;
import org.codefilarete.tool.function.Predicates;

/**
 * A class that "roughly" persists a {@link Collection} to some {@link Column}s : {@link Collection} values are written to given {@link Column}s.
 * Write is made in iteration order. One may change this behavior by overriding {@link #toCollectionValue(Object)} and {@link #toDatabaseValue(Object)}.
 * 
 * @author Guillaume Mary
 */
public class ColumnedCollectionMapping<C extends Collection<O>, O, T extends Table<T>> implements EmbeddedBeanMapping<C, T> {
	
	private final T targetTable;
	private final Set<Column<T, ?>> columns;
	private final ToCollectionRowTransformer<C> rowTransformer;
	private final Class<C> persistedClass;
	
	/**
	 * Constructor 
	 *
	 * @param targetTable table to persist in
	 * @param columns columns that will be used for persistent of Collections, expected to be a subset of targetTable columns    
	 * @param rowClass Class to instantiate for select from database
	 */
	public ColumnedCollectionMapping(T targetTable, Set<? extends Column<T, ?>> columns, Class<C> rowClass) {
		this.targetTable = targetTable;
		this.columns = (Set<Column<T, ?>>) columns;
		this.persistedClass = rowClass;
		this.rowTransformer = new LocalToCollectionRowTransformer<C>(getPersistedClass(), this::toCollectionValue);
	}
	
	public Class<C> getPersistedClass() {
		return persistedClass;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column<T, ?>> getColumns() {
		return columns;
	}
	
	@Override
	public ToCollectionRowTransformer<C> getRowTransformer() {
		return rowTransformer;
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint<C> accessor) {
		// this class doesn't support bean factory so it can't support properties set by constructor
	}
	
	@Override
	public Map<Column<T, ?>, ?> getInsertValues(C c) {
		Collection<O> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new ArrayList<>();
		}
		// NB: we wrap c.iterator() in an InfiniteIterator to get all columns generated: overflow columns will have
		// null value (see 	InfiniteIterator#getValue)
		PairIterator<Column<T, ?>, O> valueColumnPairIterator = new PairIterator<>(columns.iterator(), new InfiniteIterator<>(toIterate.iterator()));
		return Iterables.map(() -> valueColumnPairIterator, Duo::getLeft, e -> toDatabaseValue(e.getRight()));
	}
	
	@Override
	public Map<UpwhereColumn<T>, ?> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, ?>, Object> toReturn = new HashMap<>();
		if (modified != null) {
			// getting differences side by side
			Map<Column<T, ?>, O> unmodifiedColumns = new LinkedHashMap<>();
			Iterator<O> unmodifiedIterator = unmodified == null ? new EmptyIterator<>() : unmodified.iterator();
			UntilBothIterator<? extends O, ? extends O> untilBothIterator = new UntilBothIterator<>(modified.iterator(), unmodifiedIterator);
			PairIterator<Column<T, ?>, Duo<? extends O, ? extends O>> valueColumnPairIterator = new PairIterator<>(columns.iterator(), untilBothIterator);
			valueColumnPairIterator.forEachRemaining(diffEntry -> {
				Column<T, ?> fieldColumn = diffEntry.getLeft();
				Duo<? extends O, ? extends O> toBeCompared = diffEntry.getRight();
				if (!Predicates.equalOrNull(toBeCompared.getLeft(), toBeCompared.getRight())) {
					toReturn.put(fieldColumn, toDatabaseValue(toBeCompared.getLeft()));
				} else {
					unmodifiedColumns.put(fieldColumn, toBeCompared.getRight());
				}
			});
			
			// adding complementary columns if necessary
			if (allColumns && !toReturn.isEmpty()) {
				Set<Column<T, ?>> missingColumns = new LinkedHashSet<>(columns);
				missingColumns.removeAll(toReturn.keySet());
				for (Column<T, ?> missingColumn : missingColumns) {
					Object missingValue = unmodifiedColumns.get(missingColumn);
					toReturn.put(missingColumn, missingValue);
				}
			}
		} else if (allColumns && unmodified != null) {
			for (Column<T, ?> column : columns) {
				toReturn.put(column, null);
			}
		}
		
		return convertToUpwhereColumn(toReturn);
	}
	
	private Map<UpwhereColumn<T>, ?> convertToUpwhereColumn(Map<? extends Column<T, ?>, ?> map) {
		Map<UpwhereColumn<T>, Object> convertion = new HashMap<>();
		map.forEach((c, s) -> convertion.put(new UpwhereColumn<>(c, true), s));
		return convertion;
	}
	
	/**
	 * Gives the database (JDBC) value of the argument.
	 * This implementation returns the given argument without transformation.
	 * This may duplicate behavior of {@link PreparedStatementWriter} in some way, but is located to this strategy so can be
	 * more accurate.
	 * 
	 * @param object any object took from a persistent collection
	 * @return the value to be persisted
	 */
	protected Object toDatabaseValue(O object) {
		return object;
	}
	
	/**
	 * Opposit of {@link #toDatabaseValue(Object)}: converts the database value for the collection value
	 * This implementation returns the given argument without transformation.
	 * This may duplicate behavior of {@link ResultSetReader} in some way, but is located to this strategy so can be
	 * more accurate.
	 * 
	 * @param object the value coming from the database {@link java.sql.ResultSet}
	 * @return a value for a Map
	 */
	protected O toCollectionValue(Object object) {
		return (O) object;
	}
	
	@Override
	public C transform(ColumnedRow row) {
		return this.rowTransformer.transform(row);
	}
	
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getPropertyToColumn() {
		throw new UnsupportedOperationException(Reflections.toString(ColumnedCollectionMapping.class) + " can't export a mapping between some accessors and their columns");
	}
	
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getReadonlyPropertyToColumn() {
		throw new UnsupportedOperationException(Reflections.toString(ColumnedCollectionMapping.class) + " can't export a mapping between some accessors and their columns");
	}
	
	@Override
	public Set<Column<T, ?>> getWritableColumns() {
		return this.columns;
	}
	
	@Override
	public Set<Column<T, ?>> getReadonlyColumns() {
		return java.util.Collections.emptySet();
	}
	
	private class LocalToCollectionRowTransformer<C extends Collection> extends ToCollectionRowTransformer<C> {
		
		private final Function<Object, Object> databaseValueConverter;
		
		private LocalToCollectionRowTransformer(Class<C> persistedClass, Function<Object, Object> databaseValueConverter) {
			super(persistedClass);
			this.databaseValueConverter = databaseValueConverter;
		}
		
		private LocalToCollectionRowTransformer(Function<ColumnedRow, C> beanFactory,
												Function<Object, Object> databaseValueConverter) {
			super(beanFactory);
			this.databaseValueConverter = databaseValueConverter;
		}
		
		/** We bind conversion on {@link ColumnedCollectionMapping} conversion methods */
		@Override
		public void applyRowToBean(ColumnedRow row, C collection) {
			for (Column<T, ?> column : columns) {
				Object value = row.get(column);
				collection.add(this.databaseValueConverter.apply(value));
			}
		}
	}
}
