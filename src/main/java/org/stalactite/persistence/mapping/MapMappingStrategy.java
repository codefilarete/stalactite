package org.stalactite.persistence.mapping;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.bean.Objects;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class MapMappingStrategy<C extends Map<K, V>, K, V, T> implements IMappingStrategy<C> {
	
	private Set<Column> columns;
	
	private Map<String, Column> namedColumns;
	
	public MapMappingStrategy(@Nonnull Table targetTable, String columnsPrefix, @Nonnull Class<T> collectionGenericType, int nbCol) {
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
		namedColumns = targetTable.mapColumnsOnName();
	}
	
	protected String getColumnName(String columnsPrefix, int i) {
		return columnsPrefix + i;
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull C c) {
		final PersistentValues toReturn = new PersistentValues();
		Iterables.visit(c.entrySet(), new ForEach<Entry<K, V>, Void>() {
			@Override
			public Void visit(Entry<K, V> mapEntry) {
				String columnName = getColumnName(mapEntry.getKey());
				Column column = namedColumns.get(columnName);
				toReturn.putUpsertValue(column, convertMapValue(mapEntry.getValue()));
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
	public PersistentValues getUpdateValues(@Nonnull final C modified, @Nonnull C unmodified) {
		final PersistentValues toReturn = new PersistentValues();
		// Toutes celles appartenant à modified et différentes dans unmodified
		for (Entry<K, V> modifiedEntry : modified.entrySet()) {
			K modifiedKey = modifiedEntry.getKey();
			V modifiedValue = modifiedEntry.getValue();
			if (!Objects.equalsWithNull(modifiedValue, unmodified.get(modifiedKey))) {
				String columnName = getColumnName(modifiedKey);
				Column column = namedColumns.get(columnName);
				toReturn.putUpsertValue(column, modifiedValue);
			}
		}
		// Toutes celles de unmodified absentes de modified
		HashSet<K> missingInModified = new HashSet<>(unmodified.keySet());
		missingInModified.removeAll(modified.keySet());
		for (K k : missingInModified) {
			String columnName = getColumnName(k);
			Column column = namedColumns.get(columnName);
			toReturn.putUpsertValue(column, modified.get(k));
		}
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
	
	protected abstract String getColumnName(K k);
	
	protected abstract T convertMapValue(V v);
}
