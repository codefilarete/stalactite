package org.gama.stalactite.persistence.mapping;

import org.gama.lang.Reflections;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public class ClassMappingStrategy<T> implements IMappingStrategy<T> {
	
	private Class<T> classToPersist;
	
	private FieldMappingStrategy<T> defaultMappingStrategy;
	
	private final Table targetTable;
	
	private final Set<Column> columns;
	
	private final Set<Column> updatableColumns;
	
	private Map<PropertyAccessor, IEmbeddedBeanMapper> mappingStrategies;
	
	private final IdentifierGenerator identifierGenerator;
	
	public ClassMappingStrategy(@Nonnull Class<T> classToPersist, @Nonnull Table targetTable,
								Map<Field, Column> fieldToColumn, Field identifierField, IdentifierGenerator identifierGenerator) {
		this.classToPersist = classToPersist;
		this.targetTable = targetTable;
		this.defaultMappingStrategy = new FieldMappingStrategy<>(fieldToColumn, identifierField);
		this.columns = new HashSet<>(defaultMappingStrategy.getColumns());
		this.updatableColumns = new LinkedHashSet<>();
		this.mappingStrategies = new HashMap<>();
		this.identifierGenerator = identifierGenerator;
		fillUpdatableColumns();
	}
	
	public Class<T> getClassToPersist() {
		return classToPersist;
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	@Override
	public Set<Column> getColumns() {
		return columns;
	}
	
	/**
	 * Gives columns that can be updated: columns minus keys
	 * @return columns aff all mapping strategies without getKeys()
	 */
	public Set<Column> getUpdatableColumns() {
		return updatableColumns;
	}
	
	/**
	 * Indique une stratégie spécifique pour un attribut donné
	 * @param field
	 * @param mappingStrategy
	 */
	public void put(Field field, IEmbeddedBeanMapper mappingStrategy) {
		mappingStrategies.put(PropertyAccessor.forProperty(field), mappingStrategy);
		Reflections.ensureAccessible(field);
		// update columns list
		updateColumnsLists(mappingStrategy);
	}
	
	private void updateColumnsLists(IEmbeddedBeanMapper mappingStrategy) {
		columns.addAll(mappingStrategy.getColumns());
		fillUpdatableColumns(mappingStrategy);
	}
	
	public IdentifierGenerator getIdentifierGenerator() {
		return identifierGenerator;
	}
	
	@Override
	public Map<Column, Object> getInsertValues(@Nonnull T t) {
		Map<Column, Object> insertValues = defaultMappingStrategy.getInsertValues(t);
		for (Entry<PropertyAccessor, IEmbeddedBeanMapper> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue = fieldStrategyEntry.getKey().get(t);
			Map<Column, Object> fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		}
		return insertValues;
	}
	
	@Override
	public Map<UpwhereColumn, Object> getUpdateValues(@Nonnull T modified, T unmodified, boolean allColumns) {
		Map<UpwhereColumn, Object> toReturn = defaultMappingStrategy.getUpdateValues(modified, unmodified, allColumns);
		for (Entry<PropertyAccessor, IEmbeddedBeanMapper> fieldStrategyEntry : mappingStrategies.entrySet()) {
			PropertyAccessor<T, Object> accessor = fieldStrategyEntry.getKey();
			Object modifiedValue = accessor.get(modified);
			Object unmodifiedValue = unmodified == null ?  null : accessor.get(unmodified);
			Map<Column, Object> fieldUpdateValues = fieldStrategyEntry.getValue().getUpdateValues(modifiedValue, unmodifiedValue, allColumns);
			for (Entry<Column, Object> fieldUpdateValue : fieldUpdateValues.entrySet()) {
				toReturn.put(new UpwhereColumn(fieldUpdateValue.getKey(), true), fieldUpdateValue.getValue());
			}
		}
		if (allColumns && !toReturn.isEmpty()) {
			Set<Column> missingColumns = new HashSet<>(getUpdatableColumns());
			missingColumns.removeAll(UpwhereColumn.getUpdateColumns(toReturn).keySet());
			for (Column missingColumn : missingColumns) {
				toReturn.put(new UpwhereColumn(missingColumn, true), null);
			}
		}
		return toReturn;
	}
	
	/**
	 * Build columns that can be updated: columns minus keys
	 */
	private void fillUpdatableColumns() {
		updatableColumns.clear();
		updatableColumns.addAll(getColumns());
		for (IEmbeddedBeanMapper<?> iEmbeddedBeanMapper : mappingStrategies.values()) {
			fillUpdatableColumns(iEmbeddedBeanMapper);
		}
		// keys are never updated
		for (Column column : getKeys()) {
			updatableColumns.remove(column);
		}
	}
	
	private void fillUpdatableColumns(IEmbeddedBeanMapper<?> iEmbeddedBeanMapper) {
		updatableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	@Override
	public Map<Column, Object> getDeleteValues(@Nonnull T t) {
		return defaultMappingStrategy.getDeleteValues(t);
	}
	
	@Override
	public Map<Column, Object> getSelectValues(@Nonnull Serializable id) {
		return defaultMappingStrategy.getSelectValues(id);
	}
	
	@Override
	public Map<Column, Object> getVersionedKeyValues(@Nonnull T t) {
		return defaultMappingStrategy.getVersionedKeyValues(t);
	}
	
	public Iterable<Column> getVersionedKeys() {
		return defaultMappingStrategy.getVersionedKeys();
	}
	
	public Iterable<Column> getKeys() {
		return defaultMappingStrategy.getKeys();
	}
	
	public boolean isSingleColumnKey() {
		return defaultMappingStrategy.isSingleColumnKey();
	}
	
	public Column getSingleColumnKey() {
		return defaultMappingStrategy.getSingleColumnKey();
	}
	
	@Override
	public Serializable getId(T t) {
		return defaultMappingStrategy.getId(t);
	}
	
	/**
	 * Fix object id.
	 * 
	 * @param t a persistent bean 
	 * @param identifier the bean identifier, generated by IdentifierGenerator
	 */
	@Override
	public void setId(T t, Serializable identifier) {
		defaultMappingStrategy.setId(t, identifier);
	}
	
	@Override
	public T transform(Row row) {
		T toReturn = defaultMappingStrategy.transform(row);
		for (Entry<PropertyAccessor, IEmbeddedBeanMapper> mappingStrategyEntry : mappingStrategies.entrySet()) {
			mappingStrategyEntry.getKey().set(toReturn, mappingStrategyEntry.getValue().transform(row));
		}
		return toReturn;
	}
}
