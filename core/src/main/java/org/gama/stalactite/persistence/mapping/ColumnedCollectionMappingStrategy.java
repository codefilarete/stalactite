package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.collection.PairIterator.EmptyIterator;
import org.gama.lang.collection.PairIterator.InfiniteIterator;
import org.gama.lang.collection.PairIterator.UntilBothIterator;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public abstract class ColumnedCollectionMappingStrategy<C extends Collection<T>, T> implements IEmbeddedBeanMapper<C> {
	
	private final Table targetTable;
	private final Set<Column> columns;
	private final ToCollectionRowTransformer<C> rowTransformer;
	
	public ColumnedCollectionMappingStrategy(Table targetTable, Set<Column> columns, Class<? extends Collection> rowClass) {
		this.targetTable = targetTable;
		this.columns = columns;
		// weird cast cause of generics
		this.rowTransformer = new ToCollectionRowTransformer<C>((Class<C>) rowClass) {
			/** We bind conversion on CollectionColumnedMappingStrategy conversion methods */
			@Override
			protected void applyRowToBean(Row row, C collection) {
				for (Column column : getColumns()) {
					Object value = row.get(column.getName());
					collection.add(toCollectionValue(value));
				}
			}
		};
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	@Override
	public Map<Column, Object> getInsertValues(C c) {
		final Map<Column, Object> toReturn = new HashMap<>();
		Collection<T> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new ArrayList<>();
		}
		// NB: we wrap c.iterator() in an InfiniteIterator to get all columns generated: overflow columns will have
		// null value (see 	InfiniteIterator#getValue)
		PairIterator<Column, T> valueColumnPairIterator = new PairIterator<>(columns.iterator(), new InfiniteIterator<>(toIterate.iterator()));
		Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column, T>, Void>() {
			@Override
			public Void visit(Entry<Column, T> valueEntry) {
				toReturn.put(valueEntry.getKey(), toDatabaseValue(valueEntry.getValue()));
				return null;
			}
			});
		return toReturn;
	}
	
	@Override
	public Map<UpwhereColumn, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column, Object> toReturn = new HashMap<>();
		if (modified != null) {
			// getting differences side by side
			Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
			Iterator<T> unmodifiedIterator = unmodified == null ? new EmptyIterator<>() : unmodified.iterator();
			UntilBothIterator<T, T> untilBothIterator = new UntilBothIterator<>(modified.iterator(), unmodifiedIterator);
			PairIterator<Column, Entry<T, T>> valueColumnPairIterator = new PairIterator<>(columns.iterator(), untilBothIterator);
			Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column, Entry<T, T>>, Void>() {
				@Override
				public Void visit(Entry<Column, Entry<T, T>> diffEntry) {
					Column fieldColumn = diffEntry.getKey();
					Entry<T, T> toBeCompared = diffEntry.getValue();
					if (!Objects.equalsWithNull(toBeCompared.getKey(), toBeCompared.getValue())) {
						toReturn.put(fieldColumn, toDatabaseValue(toBeCompared.getKey()));
					} else {
						unmodifiedColumns.put(fieldColumn, toBeCompared.getKey());
					}
					return null;
				}
			});
			
			// adding complementary columns if necessary
			if (allColumns && !toReturn.isEmpty()) {
				Set<Column> missingColumns = new LinkedHashSet<>(columns);
				missingColumns.removeAll(toReturn.keySet());
				for (Column missingColumn : missingColumns) {
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
	
	private Map<UpwhereColumn, Object> convertToUpwhereColumn(Map<Column, Object> map) {
		Map<UpwhereColumn, Object> convertion = new HashMap<>();
		map.forEach((c, s) -> convertion.put(new UpwhereColumn(c, true), s));
		return convertion;
	}
	
	protected Object toDatabaseValue(T t) {
		return t;
	}
	
	/**
	 * Reverse of {@link #toDatabaseValue(Object)}: give a map value from a database selected value
	 * @param t
	 * @return a value for a Map
	 */
	protected abstract T toCollectionValue(Object t);
	
	@Override
	public C transform(Row row) {
		return this.rowTransformer.transform(row);
	}
}
