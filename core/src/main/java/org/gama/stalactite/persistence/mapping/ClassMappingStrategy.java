package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
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
 * <b>THIS CLASS DOESN'C ADDRESS RELATION MAPPING</b>, because it's not the purpose of Stalactite 'core' module, see 'orm' module.
 * Meanwhile one case use {@link org.gama.stalactite.query.model.Query} to construct complex type.
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.query.model.Query
 */
public class ClassMappingStrategy<C, I, T extends Table> implements IEntityMappingStrategy<C, I, T> {
	
	private final Class<C> classToPersist;
	
	private final EmbeddedBeanMappingStrategy<C, T> defaultMappingStrategy;
	
	private final T targetTable;
	
	private final Set<Column<T, Object>> insertableColumns;
	
	private final Set<Column<T, Object>> updatableColumns;
	
	private final Set<Column<T, Object>> selectableColumns;
	
	private final Map<IReversibleAccessor<C, Object>, IEmbeddedBeanMapper<Object, T>> mappingStrategies;
	
	private final IdMappingStrategy<C, I> idMappingStrategy;
	
	private final Map<PropertyAccessor, Column<T, Object>> versioningMapping = new HashMap<>();
	
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
	public ClassMappingStrategy(Class<C> classToPersist,
								T targetTable,
								Map<? extends IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
								IReversibleAccessor<C, I> identifierProperty,
								IdentifierInsertionManager<C, I> identifierInsertionManager) {
		if (identifierProperty == null) {
			throw new UnsupportedOperationException("No identifier property for " + Reflections.toString(classToPersist));
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
	
	public Class<C> getClassToPersist() {
		return classToPersist;
	}
	
	@Override
	public T getTargetTable() {
		return targetTable;
	}
	
	public EmbeddedBeanMappingStrategy<C, T> getDefaultMappingStrategy() {
		return defaultMappingStrategy;
	}
	
	/**
	 * Gives columns that can be inserted: columns minus generated keys
	 * @return columns of all mapping strategies without auto-generated keys
	 */
	public Set<Column<T, Object>> getInsertableColumns() {
		return insertableColumns;
	}
	
	/**
	 * Gives columns that can be updated: columns minus keys
	 * @return columns of all mapping strategies without getKey()
	 */
	public Set<Column<T, Object>> getUpdatableColumns() {
		return updatableColumns;
	}
	
	public Set<Column<T, Object>> getSelectableColumns() {
		return selectableColumns;
	}
	
	public IdMappingStrategy<C, I> getIdMappingStrategy() {
		return idMappingStrategy;
	}
	
	public void addVersionedColumn(PropertyAccessor propertyAccessor, Column<T, Object> column) {
		this.versioningMapping.put(propertyAccessor, column);
	}
	
	@Override
	public <O> void addSilentColumnInserter(Column<T, O> column, Function<C, O> valueProvider) {
		// we delegate value computation to the default mapping strategy
		defaultMappingStrategy.addSilentColumnInserter(column, valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		insertableColumns.add((Column<T, Object>) column);
	}
	
	@Override
	public <O> void addSilentColumnUpdater(Column<T, O> column, Function<C, O> valueProvider) {
		// we delegate value computation to the default mapping strategy
		defaultMappingStrategy.addSilentColumnUpdater(column, valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		updatableColumns.add((Column<T, Object>) column);
	}
	
//	@Override
	public <O> void addSilentColumnSelecter(Column<T, O> column) {
		// we must register it as an insertable column so we'll generate the right SQL order
		selectableColumns.add((Column<T, Object>) column);
	}
	
	/**
	 * Gives a particular strategy for a given {@link Member}.
	 * The {@link Member} is supposed to be a complex type so it needs multiple columns for persistence and then needs an {@link IEmbeddedBeanMapper}
	 * 
	 * @param property an object representing a {@link Field} or {@link Method}
	 * @param mappingStrategy the strategy that should be used to persist the member
	 */
	public <O> void put(IReversibleAccessor<C, ? extends O> property, IEmbeddedBeanMapper<? extends O, T> mappingStrategy) {
		mappingStrategies.put((IReversibleAccessor) property, (IEmbeddedBeanMapper) mappingStrategy);
		// update columns lists
		addInsertableColumns(mappingStrategy);
		addUpdatableColumns(mappingStrategy);
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Map<Column<T, Object>, Object> insertValues = defaultMappingStrategy.getInsertValues(c);
		getVersionedKeyValues(c).entrySet().stream()
				// autoincrement columns mustn'c be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		for (Entry<? extends IReversibleAccessor<C, Object>, IEmbeddedBeanMapper<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue = fieldStrategyEntry.getKey().get(c);
			Map<Column<T, Object>, Object> fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		}
		return insertValues;
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<UpwhereColumn<T>, Object> toReturn = defaultMappingStrategy.getUpdateValues(modified, unmodified, allColumns);
		for (Entry<? extends IReversibleAccessor<C, Object>, IEmbeddedBeanMapper<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
			IReversibleAccessor<C, Object> accessor = fieldStrategyEntry.getKey();
			Object modifiedValue = accessor.get(modified);
			Object unmodifiedValue = unmodified == null ?  null : accessor.get(unmodified);
			Map<UpwhereColumn<T>, Object> fieldUpdateValues = fieldStrategyEntry.getValue().getUpdateValues(modifiedValue, unmodifiedValue, allColumns);
			for (Entry<UpwhereColumn<T>, Object> fieldUpdateValue : fieldUpdateValues.entrySet()) {
				toReturn.put(fieldUpdateValue.getKey(), fieldUpdateValue.getValue());
			}
		}
		if (!toReturn.isEmpty()) {
			if (allColumns) {
				Set<Column<T, Object>> missingColumns = new HashSet<>(getUpdatableColumns());
				missingColumns.removeAll(UpwhereColumn.getUpdateColumns(toReturn).keySet());
				for (Column<T, Object> missingColumn : missingColumns) {
					toReturn.put(new UpwhereColumn<>(missingColumn, true), null);
				}
			}
			for (Entry<Column<T, Object>, Object> entry : getVersionedKeyValues(modified).entrySet()) {
				toReturn.put(new UpwhereColumn<>(entry.getKey(), false), entry.getValue());
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
		insertableColumns.removeAll(((Set<Column>) getTargetTable().getColumns()).stream().filter(Column::isAutoGenerated).collect(Collectors.toList()));
	}
	
	private void addInsertableColumns(IEmbeddedBeanMapper<?, T> iEmbeddedBeanMapper) {
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
		updatableColumns.removeAll(((Set<Column>) getTargetTable().getColumns()).stream().filter(Column::isAutoGenerated).collect(Collectors.toList()));
	}
	
	private void addUpdatableColumns(IEmbeddedBeanMapper<?, T> iEmbeddedBeanMapper) {
		updatableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	private void fillSelectableColumns() {
		selectableColumns.clear();
		addSelectableColumns(defaultMappingStrategy);
		selectableColumns.addAll(versioningMapping.values());
		mappingStrategies.values().forEach(this::addSelectableColumns);
	}
	
	private void addSelectableColumns(IEmbeddedBeanMapper<?, T> iEmbeddedBeanMapper) {
		selectableColumns.addAll(iEmbeddedBeanMapper.getColumns());
	}
	
	public Map<Column<T, Object>, Object> getVersionedKeyValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		toReturn.put(this.targetTable.getPrimaryKey(), getId(c));
		toReturn.putAll(getVersionedColumnsValues(c));
		return toReturn;
	}
	
	private Map<Column<T, Object>, Object> getVersionedColumnsValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		for (Entry<PropertyAccessor, Column<T, Object>> columnEntry : versioningMapping.entrySet()) {
			toReturn.put(columnEntry.getValue(), columnEntry.getKey().get(c));
		}
		return toReturn;
	}
	
	public Iterable<Column<T, Object>> getVersionedKeys() {
		HashSet<Column<T, Object>> columns = new HashSet<>(versioningMapping.values());
		columns.add(this.targetTable.getPrimaryKey());
		return Collections.unmodifiableSet(columns);
	}
	
	@Override
	public I getId(C c) {
		return getIdMappingStrategy().getId(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		getIdMappingStrategy().setId(c, identifier);
	}
	
	@Override
	public boolean isNew(C c) {
		return getIdMappingStrategy().isNew(c);
	}
	
	@Override
	public C transform(Row row) {
		C toReturn = defaultMappingStrategy.transform(row);
		for (Entry<? extends IReversibleAccessor<C, Object>, IEmbeddedBeanMapper<Object, T>> mappingStrategyEntry : mappingStrategies.entrySet()) {
			mappingStrategyEntry.getKey().toMutator().set(toReturn, mappingStrategyEntry.getValue().transform(row));
		}
		return toReturn;
	}
	
	public ToBeanRowTransformer<C> getRowTransformer() {
		return defaultMappingStrategy.getRowTransformer();
	}
	
}
