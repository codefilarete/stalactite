package org.stalactite.persistence.mapping;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.bean.Objects;
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
public class CollectionColumnedMappingStrategy<C extends Collection<T>, T> implements IMappingStrategy<C> {
	
	private Set<Column> columns;
	
	public CollectionColumnedMappingStrategy(@Nonnull Table targetTable, String columnsPrefix, @Nonnull Class<T> collectionGenericType, int nbCol) {
		Map<String, Column> existingColumns = targetTable.mapColumnsOnName();
		columns = new LinkedHashSet<>(nbCol, 1);
		for (int i = 1; i <= nbCol; i++) {
			String columnName = getColumnName(columnsPrefix, i);
			Column column = existingColumns.get(columnName);
			if (column == null) {
				column = targetTable.new Column(columnName, collectionGenericType);
			}
			columns.add(column);
		}
	}
	
	protected String getColumnName(String columnsPrefix, int i) {
		return columnsPrefix + i;
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull C c) {
		final PersistentValues toReturn = new PersistentValues();
		// NB: on englobe c.iterator() dans un InfiniteIterator pour avoir toutes les colonnes générées: celles en
		// surplus auront une valeur null (cf InfiniteIterator#getValue)
		PairIterator<Column, T> valueColumnPairIterator = new PairIterator<>(columns.iterator(), new InfiniteIterator<>(c.iterator()));
		Iterables.visit(valueColumnPairIterator, new ForEach<Entry<Column, T>, Void>() {
			@Override
			public Void visit(Entry<Column, T> valueEntry) {
				toReturn.putUpsertValue(valueEntry.getKey(), valueEntry.getValue());
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
					toReturn.putUpsertValue(column, toBeCompared.getKey());
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
	
	protected Object getValue(T t) {
		return t;
	}
}
