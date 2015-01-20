package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.bean.Objects;
import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class MapMappingStrategy<C extends Map<K, V>, K, V, T> implements IMappingStrategy<C> {
	
	private final Table targetTable;
	private final Set<Column> columns;
	private final Class<T> persistentType;
	
	public MapMappingStrategy(@Nonnull Table targetTable, @Nonnull Class<T> persistentType) {
		this.targetTable = targetTable;
		this.columns = initTargetColumns();
		this.persistentType = persistentType;
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	public Class<T> getPersistentType() {
		return persistentType;
	}
	
	protected String getColumnName(String columnsPrefix, int i) {
		return columnsPrefix + i;
	}
	
	@Override
	public PersistentValues getInsertValues(@Nonnull C c) {
		final PersistentValues toReturn = new PersistentValues();
		Map<K, V> toIterate = c;
		if (Collections.isEmpty(c)) {
			toIterate = new HashMap<>();
		}
		Iterables.visit(toIterate.entrySet(), new ForEach<Entry<K, V>, Void>() {
			@Override
			public Void visit(Entry<K, V> mapEntry) {
				Column column = getColumn(mapEntry.getKey());
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
	public PersistentValues getUpdateValues(final C modified, C unmodified, boolean allColumns) {
		final Map<Column, Object> unmodifiedColumns = new LinkedHashMap<>();
		final PersistentValues toReturn = new PersistentValues();
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
	
	protected PersistentValues getDifferences(C modified, C unmodified) {
		final PersistentValues toReturn = new PersistentValues();
		if (modified != null) {
			// Toutes celles appartenant à modified et différentes dans unmodified
			for (Entry<K, V> modifiedEntry : modified.entrySet()) {
				K modifiedKey = modifiedEntry.getKey();
				V modifiedValue = modifiedEntry.getValue();
				if (!Objects.equalsWithNull(modifiedValue, unmodified == null ? null : unmodified.get(modifiedKey))) {
					Column column = getColumn(modifiedKey);
					toReturn.putUpsertValue(column, modifiedValue);
				}
			}
			// Toutes celles de unmodified absentes de modified
			HashSet<K> missingInModified = unmodified == null ? new HashSet<K>() : new HashSet<>(unmodified.keySet());
			missingInModified.removeAll(modified.keySet());
			for (K k : missingInModified) {
				Column column = getColumn(k);
				toReturn.putUpsertValue(column, modified.get(k));
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
		return new UnsupportedOperationException("Map strategy can't give id");
	}
	
	protected abstract LinkedHashSet<Column> initTargetColumns();
	
	protected abstract Column getColumn(K k);
	
	protected abstract T convertMapValue(V v);
}
