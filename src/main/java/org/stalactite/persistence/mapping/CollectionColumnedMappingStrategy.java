package org.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.bean.Objects;
import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.collection.PairIterator;
import org.stalactite.lang.collection.PairIterator.InfiniteIterator;
import org.stalactite.lang.collection.PairIterator.UntilBothIterator;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class CollectionColumnedMappingStrategy<C extends Collection<T>, T> implements IMappingStrategy<C> {
	
	private final Table targetTable;
	private final Set<Column> columns;
	private final Class<T> persistentType;
	
//	public CollectionColumnedMappingStrategy(@Nonnull Table targetTable, String columnsPrefix, @Nonnull Class<T> collectionGenericType, int nbCol) {
//		this.targetTable = targetTable;
//		Map<String, Column> existingColumns = this.targetTable.mapColumnsOnName();
//		columns = new LinkedHashSet<>(nbCol, 1);
//		for (int i = 1; i <= nbCol; i++) {
//			String columnName = getColumnName(columnsPrefix, i);
//			Column column = existingColumns.get(columnName);
//			if (column == null) {
//				column = targetTable.new Column(columnName, collectionGenericType);
//			}
//			columns.add(column);
//		}
//	}
	
	public CollectionColumnedMappingStrategy(@Nonnull Table targetTable, @Nonnull Class<T> persistentType) {
		this.targetTable = targetTable;
		this.columns = initTargetColumns();
		this.persistentType = persistentType;
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	public Class<T> getPersistentType() {
		return persistentType;
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull C c) {
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
				toReturn.putUpsertValue(valueEntry.getKey(), getValue(valueEntry.getValue()));
				return null;
			}
			});
		return toReturn;
	}
	
	@Override
	public PersistentValues getUpdateValues(@Nonnull C modified, @Nonnull C unmodified) {
		final PersistentValues toReturn = new PersistentValues();
		UntilBothIterator<T, T> untilBothIterator = new UntilBothIterator<>(modified, unmodified);
		PairIterator<Column, Entry<T, T>> valueColumnPairIterator = new PairIterator<>(columns.iterator(), untilBothIterator);
		Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column,Entry<T,T>>, Void>() {
			@Override
			public Void visit(Entry<Column, Entry<T, T>> diffEntry) {
				Column column = diffEntry.getKey();
				Entry<T, T> toBeCompared = diffEntry.getValue();
				if (!Objects.equalsWithNull(toBeCompared.getKey(), toBeCompared.getValue())) {
					toReturn.putUpsertValue(column, getValue(toBeCompared.getKey()));
				}
				return null;
			}
		});
		return toReturn;
	}
	
	@Override
	public PersistentValues getDeleteValues(@Nonnull C c) {
		// Pas de valeur pour le where de suppression
		return new PersistentValues();
	}
	
	@Override
	public PersistentValues getSelectValues(@Nonnull C c) {
		// Pas de valeur pour le where de sélection
		return new PersistentValues();
	}
	
	@Override
	public PersistentValues getVersionedKeyValues(@Nonnull C c) {
		// Pas de valeur pour la clé versionnée
		return new PersistentValues();
	}
	
	protected abstract LinkedHashSet<Column> initTargetColumns();
	
	protected Object getValue(T t) {
		return t;
	}
}
