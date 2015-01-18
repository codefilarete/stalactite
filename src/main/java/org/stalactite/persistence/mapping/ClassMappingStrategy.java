package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.lang.Reflections;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class ClassMappingStrategy<T> implements IMappingStrategy<T> {
	
	private Class<T> classToPersist;
	
	private FieldMappingStrategy<T> defaultMappingStrategy;
	
	private final Table targetTable;
	
	private final Set<Column> columns;
	
	private Map<Field, IMappingStrategy> mappingStrategies;
	
	public ClassMappingStrategy(@Nonnull Class<T> classToPersist, @Nonnull Table targetTable, Map<Field, Column> fieldToColumn) {
		this.classToPersist = classToPersist;
		this.targetTable = targetTable;
		this.defaultMappingStrategy = new FieldMappingStrategy<>(fieldToColumn);
		this.columns = new HashSet<>(defaultMappingStrategy.getColumns());
		this.mappingStrategies = new HashMap<>();
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
	public PersistentValues getInsertValues(@Nonnull T t) {
		PersistentValues insertValues = defaultMappingStrategy.getInsertValues(t);
		for (Entry<Field, IMappingStrategy> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue;
			try {
				fieldValue = fieldStrategyEntry.getKey().get(t);
			} catch (IllegalAccessException e) {
				// Shouldn't happen
				throw new RuntimeException(e);
			}
			PersistentValues fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.getUpsertValues().putAll(fieldInsertValues.getUpsertValues());
		}
		return insertValues;
	}
	
	@Override
	public PersistentValues getUpdateValues(@Nonnull T modified, T unmodified, boolean allColumns) {
		PersistentValues toReturn = defaultMappingStrategy.getUpdateValues(modified, unmodified, allColumns);
		for (Entry<Field, IMappingStrategy> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object modifiedValue, unmodifiedValue;
			try {
				Field field = fieldStrategyEntry.getKey();
				modifiedValue = field.get(modified);
				unmodifiedValue = unmodified == null ?  null : field.get(unmodified);
			} catch (IllegalAccessException e) {
				// Shouldn't happen
				throw new RuntimeException(e);
			}
			PersistentValues fieldUpdateValues = fieldStrategyEntry.getValue().getUpdateValues(modifiedValue, unmodifiedValue, allColumns);
			toReturn.getUpsertValues().putAll(fieldUpdateValues.getUpsertValues());
		}
		if (allColumns && !toReturn.getUpsertValues().isEmpty()) {
			Set<Column> missingColumns = new LinkedHashSet<>(getColumns());
			missingColumns.remove(getTargetTable().getPrimaryKey());	// primary key is never updated
			missingColumns.removeAll(toReturn.getUpsertValues().keySet());
			for (Column missingColumn : missingColumns) {
				toReturn.putUpsertValue(missingColumn, null);
			}
		}
		return toReturn;
	}
	
	@Override
	public PersistentValues getDeleteValues(@Nonnull T t) {
		return defaultMappingStrategy.getDeleteValues(t);
	}
	
	@Override
	public PersistentValues getSelectValues(@Nonnull T t) {
		PersistentValues selectValues = defaultMappingStrategy.getSelectValues(t);
		for (Entry<Field, IMappingStrategy> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue;
			try {
				fieldValue = fieldStrategyEntry.getKey().get(t);
			} catch (IllegalAccessException e) {
				// Shouldn't happen
				throw new RuntimeException(e);
			}
			PersistentValues fieldSelectValues = fieldStrategyEntry.getValue().getSelectValues(fieldValue);
			selectValues.getUpsertValues().putAll(fieldSelectValues.getUpsertValues());
			selectValues.getWhereValues().putAll(fieldSelectValues.getWhereValues());
		}
		return selectValues;
	}
	
	@Override
	public PersistentValues getVersionedKeyValues(@Nonnull T t) {
		return defaultMappingStrategy.getVersionedKeyValues(t);
	}
	
	@Override
	public Serializable getId(T t) {
		return defaultMappingStrategy.getId(t);
	}
	
	/**
	 * Indique une stratégie spécifique pour un attribut donnée
	 * @param field
	 * @param mappingStrategy
	 */
	public void put(Field field, IMappingStrategy mappingStrategy) {
		mappingStrategies.put(field, mappingStrategy);
		Reflections.ensureAccessible(field);
		// update columns list
		columns.addAll(mappingStrategy.getColumns());
	}
}
