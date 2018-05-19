package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.IReversibleMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * <p>
 * Main class for persistence entity mapping description.
 * Composed of:
 * <ul>
 * <li>a main strategy : an embedded one ({@link EmbeddedBeanMappingStrategy}, accessible by {@link #getDefaultMappingStrategy()}</li>
 * <li>an id strategy : {@link IdMappingStrategy} accessible with {@link #getIdMappingStrategy()}</li>
 * <li>optional version mapping : accessible with {@link #getVersionedKeys()} for instance</li>
 * <li>additionnal mappings (for embeddable for instance) : see {@link #put(IReversibleAccessor, IEmbeddedBeanMapper)}</li>
 * </ul>
 * </p>
 * <p>
 * Note that mapping is defined through getter ({@link IReversibleAccessor}) and not setter ({@link IReversibleMutator}) only
 * because a choice must be done between them, else the concrete class {@link PropertyAccessor} is the choice, which is less open.
 * </p>
 *
 * <p>
 * Mapping definition can be eased thanks to {@link PersistentFieldHarverster}, {@link org.gama.reflection.Accessors}
 * </p>
 * 
 * <br/>
 * <b>THIS CLASS DOESN'T ADRESS RELATION MAPPING</b>, because it's not the purpose of Stalactite 'core' module, see 'orm' module.
 * Meanwhile one case use {@link org.gama.stalactite.query.model.Query} to construct complex type.
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.query.model.Query
 */
public class ClassMappingStrategy<T, I> implements IEntityMappingStrategy<T, I> {
	
	private final Class<T> classToPersist;
	
	private final EmbeddedBeanMappingStrategy<T> defaultMappingStrategy;
	
	private final Table targetTable;
	
	private final Set<Column> insertableColumns;
	
	private final Set<Column> updatableColumns;
	
	private final Set<Column> selectableColumns;
	
	private final Map<IReversibleAccessor, IEmbeddedBeanMapper> mappingStrategies;
	
	private final IdMappingStrategy<T, I> idMappingStrategy;
	
	private final Map<PropertyAccessor, Column> versioningMapping = new HashMap<>();
	
	/**
	 * Only constructor to define the persistent mapping between a class and a table.
	 * It only defines main class and table, secondary (such as embedded class or
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param identifierProperty identifier of the persisted class
	 * @param identifierInsertionManager manager of identifiers
	 */
	public ClassMappingStrategy(Class<T> classToPersist, Table targetTable, Map<? extends IReversibleAccessor, Column> propertyToColumn,
								IReversibleAccessor<T, I> identifierProperty, IdentifierInsertionManager<T, I> identifierInsertionManager) {
		if (identifierProperty == null) {
			throw new UnsupportedOperationException("No identifier property for " + classToPersist.getName());
		}
		if (targetTable.getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + targetTable.getAbsoluteName());
		}
		this.targetTable = targetTable;
		this.classToPersist = classToPersist;
		this.defaultMappingStrategy = new EmbeddedBeanMappingStrategy<>(classToPersist, propertyToColumn);
		this.insertableColumns = new LinkedHashSet<>();
		this.updatableColumns = new LinkedHashSet<>();
		this.selectableColumns = new LinkedHashSet<>();
		this.mappingStrategies = new HashMap<>();
		this.idMappingStrategy = new IdMappingStrategy<>(identifierProperty, identifierInsertionManager);
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
		// identifierAccessor must be the same instance as those stored in propertyToColumn for Map.remove method used in foreach()
		Column identifierColumn = propertyToColumn.get(identifierProperty);
		if (identifierColumn == null) {
			throw new IllegalArgumentException("Bean identifier '" + identifierProperty + "' must have its matching column in the mapping");
		}
		if (!identifierColumn.isPrimaryKey()) {
			throw new UnsupportedOperationException("Accessor '" + identifierProperty
					+ "' is declared as identifier but mapped column " + identifierColumn.toString() + " is not the primary key of table");
		}
	}
	
	public Class<T> getClassToPersist() {
		return classToPersist;
	}
	
	@Override
	public Table getTargetTable() {
		return targetTable;
	}
	
	public EmbeddedBeanMappingStrategy<T> getDefaultMappingStrategy() {
		return defaultMappingStrategy;
	}
	
	/**
	 * Gives columns that can be inserted: columns minus generated keys
	 * @return columns of all mapping strategies without auto-generated keys
	 */
	public Set<Column> getInsertableColumns() {
		return insertableColumns;
	}
	
	/**
	 * Gives columns that can be updated: columns minus keys
	 * @return columns of all mapping strategies without getKey()
	 */
	public Set<Column> getUpdatableColumns() {
		return updatableColumns;
	}
	
	public Set<Column> getSelectableColumns() {
		return selectableColumns;
	}
	
	public IdMappingStrategy<T, I> getIdMappingStrategy() {
		return idMappingStrategy;
	}
	
	public void addVersionedColumn(PropertyAccessor propertyAccessor, Column column) {
		this.versioningMapping.put(propertyAccessor, column);
	}
	
	/**
	 * Gives a particular strategy for a given {@link Member}.
	 * The {@link Member} is supposed to be a complex type so it needs multiple columns for persistence and then needs an {@link IEmbeddedBeanMapper}
	 * 
	 * @param property an object representing a {@link Field} or {@link Method}
	 * @param mappingStrategy the strategy that should be used to persist the member
	 */
	public void put(IReversibleAccessor property, IEmbeddedBeanMapper mappingStrategy) {
		mappingStrategies.put(property, mappingStrategy);
		// update columns lists
		addInsertableColumns(mappingStrategy);
		addUpdatableColumns(mappingStrategy);
	}
	
	@Override
	public Map<Column, Object> getInsertValues(T t) {
		Map<Column, Object> insertValues = defaultMappingStrategy.getInsertValues(t);
		getVersionedKeyValues(t).entrySet().stream()
				// autoincrement columns mustn't be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		for (Entry<? extends IReversibleAccessor, IEmbeddedBeanMapper> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue = fieldStrategyEntry.getKey().get(t);
			Map<Column, Object> fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		}
		return insertValues;
	}
	
	@Override
	public Map<UpwhereColumn, Object> getUpdateValues(T modified, T unmodified, boolean allColumns) {
		Map<UpwhereColumn, Object> toReturn = defaultMappingStrategy.getUpdateValues(modified, unmodified, allColumns);
		for (Entry<? extends IReversibleAccessor, IEmbeddedBeanMapper> fieldStrategyEntry : mappingStrategies.entrySet()) {
			IReversibleAccessor<T, Object> accessor = fieldStrategyEntry.getKey();
			Object modifiedValue = accessor.get(modified);
			Object unmodifiedValue = unmodified == null ?  null : accessor.get(unmodified);
			Map<UpwhereColumn, Object> fieldUpdateValues = fieldStrategyEntry.getValue().getUpdateValues(modifiedValue, unmodifiedValue, allColumns);
			for (Entry<UpwhereColumn, Object> fieldUpdateValue : fieldUpdateValues.entrySet()) {
				toReturn.put(fieldUpdateValue.getKey(), fieldUpdateValue.getValue());
			}
		}
		if (!toReturn.isEmpty()) {
			if (allColumns) {
				Set<Column> missingColumns = new HashSet<>(getUpdatableColumns());
				missingColumns.removeAll(UpwhereColumn.getUpdateColumns(toReturn).keySet());
				for (Column missingColumn : missingColumns) {
					toReturn.put(new UpwhereColumn(missingColumn, true), null);
				}
			}
			for (Entry<Column, Object> entry : getVersionedKeyValues(modified).entrySet()) {
				toReturn.put(new UpwhereColumn(entry.getKey(), false), entry.getValue());
			}
		}
		return toReturn;
	}
	
	/**
	 * Build columns that can be inserted: columns minus generated keys
	 */
	private void fillInsertableColumns() {
		insertableColumns.clear();
		addInsertableColumns(defaultMappingStrategy);
		mappingStrategies.values().forEach(this::addInsertableColumns);
		insertableColumns.addAll(versioningMapping.values());
		// generated keys are never inserted
		insertableColumns.removeAll(getTargetTable().getColumns().stream().filter(Column::isAutoGenerated).collect(Collectors.toList()));
	}
	
	private void addInsertableColumns(IEmbeddedBeanMapper<?> iEmbeddedBeanMapper) {
		insertableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	/**
	 * Build columns that can be updated: columns minus keys
	 */
	private void fillUpdatableColumns() {
		updatableColumns.clear();
		addUpdatableColumns(defaultMappingStrategy);
		mappingStrategies.values().forEach(this::addUpdatableColumns);
		updatableColumns.addAll(versioningMapping.values());
		// keys are never updated
		updatableColumns.remove(getTargetTable().getPrimaryKey());
		updatableColumns.removeAll(getTargetTable().getColumns().stream().filter(Column::isAutoGenerated).collect(Collectors.toList()));
	}
	
	private void addUpdatableColumns(IEmbeddedBeanMapper<?> iEmbeddedBeanMapper) {
		updatableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	private void fillSelectableColumns() {
		selectableColumns.clear();
		addSelectableColumns(defaultMappingStrategy);
		selectableColumns.addAll(versioningMapping.values());
		mappingStrategies.values().forEach(this::addSelectableColumns);
	}
	
	private void addSelectableColumns(IEmbeddedBeanMapper<?> iEmbeddedBeanMapper) {
		selectableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	public Map<Column, Object> getVersionedKeyValues(T t) {
		Map<Column, Object> toReturn = new HashMap<>();
		toReturn.put(this.targetTable.getPrimaryKey(), getId(t));
		toReturn.putAll(getVersionedColumnsValues(t));
		return toReturn;
	}
	
	private Map<Column, Object> getVersionedColumnsValues(T t) {
		Map<Column, Object> toReturn = new HashMap<>();
		for (Entry<PropertyAccessor, Column> columnEntry : versioningMapping.entrySet()) {
			toReturn.put(columnEntry.getValue(), columnEntry.getKey().get(t));
		}
		return toReturn;
	}
	
	public Iterable<Column> getVersionedKeys() {
		HashSet<Column> columns = new HashSet<>(versioningMapping.values());
		columns.add(this.targetTable.getPrimaryKey());
		return Collections.unmodifiableSet(columns);
	}
	
	@Override
	public I getId(T t) {
		return getIdMappingStrategy().getId(t);
	}
	
	@Override
	public void setId(T t, I identifier) {
		getIdMappingStrategy().setId(t, identifier);
	}
	
	@Override
	public boolean isNew(T t) {
		return getIdMappingStrategy().isNew(t);
	}
	
	@Override
	public T transform(Row row) {
		T toReturn = defaultMappingStrategy.transform(row);
		for (Entry<? extends IReversibleAccessor, IEmbeddedBeanMapper> mappingStrategyEntry : mappingStrategies.entrySet()) {
			mappingStrategyEntry.getKey().toMutator().set(toReturn, mappingStrategyEntry.getValue().transform(row));
		}
		return toReturn;
	}
	
	public ToBeanRowTransformer<T> getRowTransformer() {
		return defaultMappingStrategy.getRowTransformer();
	}
	
}
