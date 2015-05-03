package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.collection.PairIterator.EmptyIterator;
import org.gama.lang.collection.PairIterator.InfiniteIterator;
import org.gama.lang.collection.PairIterator.UntilBothIterator;
import org.stalactite.persistence.sql.result.Row;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class ColumnedCollectionMappingStrategy<C extends Collection<T>, T> implements IMappingStrategy<C> {
	
	private final Table targetTable;
	private final Set<Column> columns;
	private final ToCollectionRowTransformer<C> rowTransformer;
	
	public ColumnedCollectionMappingStrategy(@Nonnull Table targetTable, Set<Column> columns, Class<? extends Collection> rowClass) {
		this.targetTable = targetTable;
		this.columns = columns;
		// weird cast cause of generics
		this.rowTransformer = new ToCollectionRowTransformer<C>((Class<C>) rowClass) {
			/** We bind conversion on CollectionColumnedMappingStrategy conversion methods */
			@Override
			protected void convertRowContentToMap(Row row, C collection) {
				for (Column column : getColumns()) {
					Object value = row.get(column.getName());
					collection.add(toCollectionValue(value));
				}
			}
		};
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	@Override
	public PersistentValues getInsertValues(C c) {
		final PersistentValues toReturn = new PersistentValues();
		Collection<T> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new ArrayList<>();
		}
		// NB: on englobe c.iterator() dans un InfiniteIterator pour avoir toutes les colonnes générées: celles en
		// surplus auront une valeur null (cf InfiniteIterator#getValue)
		PairIterator<Column, T> valueColumnPairIterator = new PairIterator<>(columns.iterator(), new InfiniteIterator<>(toIterate.iterator()));
		Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column, T>, Void>() {
			@Override
			public Void visit(Entry<Column, T> valueEntry) {
				toReturn.putUpsertValue(valueEntry.getKey(), toDatabaseValue(valueEntry.getValue()));
				return null;
			}
			});
		return toReturn;
	}
	
	@Override
	public PersistentValues getUpdateValues(C modified, C unmodified, boolean allColumns) {
		final PersistentValues toReturn = new PersistentValues();
		if (modified != null) {
			// getting differences side by side
			final Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
			final Iterator<T> unmodifiedIterator = unmodified == null ? new EmptyIterator<T>() : unmodified.iterator();
			UntilBothIterator<T, T> untilBothIterator = new UntilBothIterator<>(modified.iterator(), unmodifiedIterator);
			PairIterator<Column, Entry<T, T>> valueColumnPairIterator = new PairIterator<>(columns.iterator(), untilBothIterator);
			Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column, Entry<T, T>>, Void>() {
				@Override
				public Void visit(Entry<Column, Entry<T, T>> diffEntry) {
					Column fieldColumn = diffEntry.getKey();
					Entry<T, T> toBeCompared = diffEntry.getValue();
					if (!Objects.equalsWithNull(toBeCompared.getKey(), toBeCompared.getValue())) {
						toReturn.putUpsertValue(fieldColumn, toDatabaseValue(toBeCompared.getKey()));
					} else {
						unmodifiedColumns.put(fieldColumn, toBeCompared.getKey());
					}
					return null;
				}
			});
			
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
	
	@Override
	public PersistentValues getDeleteValues(@Nonnull C c) {
		// Pas de valeur pour le where de suppression
		return new PersistentValues();
	}
	
	@Override
	public PersistentValues getSelectValues(@Nonnull Serializable id) {
		// Pas de valeur pour le where de sélection
		return new PersistentValues();
	}
	
	@Override
	public PersistentValues getVersionedKeyValues(@Nonnull C c) {
		// Pas de valeur pour la clé versionnée
		return new PersistentValues();
	}
	
	@Override
	public Serializable getId(C t) {
		throw new UnsupportedOperationException("Collection strategy can't provide id");
	}
	
	@Override
	public void setId(C t, Serializable identifier) {
		throw new UnsupportedOperationException("Collection strategy can't set id");
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
