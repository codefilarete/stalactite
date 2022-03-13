package org.codefilarete.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.tool.Reflections;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.persistence.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.persistence.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.persistence.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * <p>
 * Main class for persistence entity mapping description.
 * Composed of:
 * <ul>
 * <li>a main strategy : an embedded one ({@link EmbeddedClassMappingStrategy}, accessible by {@link #getMainMappingStrategy()}</li>
 * <li>an id strategy : {@link SimpleIdMappingStrategy} accessible with {@link #getIdMappingStrategy()}</li>
 * <li>optional version mapping : accessible with {@link #getVersionedKeys()} for instance</li>
 * <li>additional mappings (for embeddable for instance) : see {@link #put(ReversibleAccessor, EmbeddedBeanMappingStrategy)}</li>
 * </ul>
 * </p>
 * <p>
 * Note that mapping is defined through getter ({@link ReversibleAccessor}) and not setter ({@link ReversibleMutator}) only
 * because a choice must be done between them, else the concrete class {@link PropertyAccessor} is the choice, which is less open.
 * </p>
 *
 * <p>
 * Mapping definition can be eased thanks to {@link PersistentFieldHarverster} or {@link Accessors}
 * </p>
 * 
 * <br/>
 * <b>THIS CLASS DOESN'T ADDRESS RELATION MAPPING</b>, because it's not the purpose of Stalactite 'core' module, see 'orm' module.
 * Meanwhile one can use {@link org.codefilarete.stalactite.query.model.Query} to construct complex type.
 * 
 * @author Guillaume Mary
 * @see org.codefilarete.stalactite.query.model.Query
 */
public class ClassMappingStrategy<C, I, T extends Table> implements EntityMappingStrategy<C, I, T> {
	
	private final EmbeddedClassMappingStrategy<C, T> mainMappingStrategy;
	
	private final Set<Column<T, Object>> insertableColumns = new LinkedHashSet<>();
	
	private final Set<Column<T, Object>> updatableColumns = new LinkedHashSet<>();
	
	private final Set<Column<T, Object>> selectableColumns = new LinkedHashSet<>();
	
	private final Map<ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> mappingStrategies = new HashMap<>();
	
	private final IdMappingStrategy<C, I> idMappingStrategy;
	
	private final Map<ReversibleAccessor, Column<T, Object>> versioningMapping = new HashMap<>();
	
	/**
	 * Main constructor
	 * Oriented for single column identifier / primary key. Prefer {@link #ClassMappingStrategy(Class, Table, Map, IdMappingStrategy)} for composed id.
	 * It only defines main class and table, secondary ones, such as embedded class, must be defined through {@link #put(ReversibleAccessor, EmbeddedBeanMappingStrategy)}
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param identifierProperty identifier of the persisted class
	 * @param identifierInsertionManager manager of identifiers
	 * @see #put(ReversibleAccessor, EmbeddedBeanMappingStrategy) 
	 */
	public ClassMappingStrategy(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
								ReversibleAccessor<C, I> identifierProperty,
								IdentifierInsertionManager<C, I> identifierInsertionManager) {
		if (identifierProperty == null) {
			throw new UnsupportedOperationException("No identifier property for " + Reflections.toString(classToPersist));
		}
		if (targetTable.getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + targetTable.getAbsoluteName());
		}
		this.mainMappingStrategy = new EmbeddedClassMappingStrategy<>(classToPersist, targetTable, propertyToColumn);
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
		// identifierAccessor must be the same instance as those stored in propertyToColumn for Map.remove method used in foreach()
		Column<T, I> identifierColumn = (Column<T, I>) propertyToColumn.get(identifierProperty);
		if (identifierColumn == null) {
			throw new IllegalArgumentException("Bean identifier '" + AccessorDefinition.toString(identifierProperty) + "' must have its matching column in the mapping");
		}
		if (!identifierColumn.isPrimaryKey()) {
			throw new UnsupportedOperationException("Accessor '" + AccessorDefinition.toString(identifierProperty)
					+ "' is declared as identifier but mapped column " + identifierColumn.toString() + " is not the primary key of table");
		}
		this.idMappingStrategy = new SimpleIdMappingStrategy<>(identifierProperty, identifierInsertionManager, new SimpleIdentifierAssembler<>(identifierColumn));
	}
	
	/**
	 * Secondary constructor, for composed id because one can precisely define the {@link IdMappingStrategy} by giving a {@link ComposedIdMappingStrategy}
	 * for instance.
	 * It only defines main class and table, secondary ones, such as embedded class, must be defined through {@link #put(ReversibleAccessor, EmbeddedBeanMappingStrategy)}
	 *
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param idMappingStrategy mapping strategy of class identifier
	 * @see #put(ReversibleAccessor, EmbeddedBeanMappingStrategy)
	 */
	public ClassMappingStrategy(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
								IdMappingStrategy<C, I> idMappingStrategy) {
		this(new EmbeddedClassMappingStrategy<>(classToPersist, targetTable, propertyToColumn), idMappingStrategy);
	}
	
	/**
	 * Constructor that let caller give a bean factory. The bean factory is given a value provider of the form of a {@link Function&lt;Column, Object&gt;}
	 * that can be called to get values to build expected entity or determine the right type to be instanciated according to current row values.
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param idMappingStrategy mapping strategy of class identifier
	 * @param entityFactory entity factory
	 * @see #put(ReversibleAccessor, EmbeddedBeanMappingStrategy)
	 */
	public ClassMappingStrategy(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
								IdMappingStrategy<C, I> idMappingStrategy,
								Function<Function<Column, Object>, C> entityFactory) {
		this(new EmbeddedClassMappingStrategy<>(classToPersist, targetTable, propertyToColumn, entityFactory), idMappingStrategy);
	}
	
	private ClassMappingStrategy(EmbeddedClassMappingStrategy<C, T> mainMappingStrategy, IdMappingStrategy<C, I> idMappingStrategy) {
		if (idMappingStrategy.getIdAccessor() == null) {
			throw new UnsupportedOperationException("No identifier property defined for " + Reflections.toString(mainMappingStrategy.getClassToPersist()));
		}
		if (mainMappingStrategy.getTargetTable().getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + mainMappingStrategy.getTargetTable().getAbsoluteName());
		}
		this.mainMappingStrategy = mainMappingStrategy;
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
		this.idMappingStrategy = idMappingStrategy;
	}
	
	public Class<C> getClassToPersist() {
		return mainMappingStrategy.getClassToPersist();
	}
	
	@Override
	public T getTargetTable() {
		return mainMappingStrategy.getTargetTable();
	}
	
	public EmbeddedClassMappingStrategy<C, T> getMainMappingStrategy() {
		return mainMappingStrategy;
	}
	
	@Override
	public Map<ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> getEmbeddedBeanStrategies() {
		return mappingStrategies;
	}
	
	/**
	 * Implementation which returns all properties mapping, even embedded ones.
	 * Result is built dynamically.
	 * 
	 * @return all properties mapping, even embedded ones
	 */
	@Override
	public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		Map<ReversibleAccessor<C, Object>, Column<T, Object>> result = new HashMap<>();
		result.putAll(getMainMappingStrategy().getPropertyToColumn());
		for (Entry<ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> value : mappingStrategies.entrySet()) {
			value.getValue().getPropertyToColumn().forEach((k, v) -> result.put(new AccessorChain<>(value.getKey(), k), v));
		}
		return result;
	}
	
	/**
	 * Gives columns that can be inserted: columns minus generated keys
	 * @return columns of all mapping strategies without auto-generated keys, as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, Object>> getInsertableColumns() {
		return Collections.unmodifiableSet(insertableColumns);
	}
	
	/**
	 * Gives columns that can be updated: columns minus keys
	 * @return columns of all mapping strategies without getKey(), as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, Object>> getUpdatableColumns() {
		return Collections.unmodifiableSet(updatableColumns);
	}
	
	/**
	 * Gives columns that can be selected
	 * @return columns of all mapping strategies, as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, Object>> getSelectableColumns() {
		return Collections.unmodifiableSet(selectableColumns);
	}
	
	@Override
	public IdMappingStrategy<C, I> getIdMappingStrategy() {
		return idMappingStrategy;
	}
	
	public Collection<ShadowColumnValueProvider<C, Object, T>> getShadowColumnsForInsert() {
		return Collections.unmodifiableCollection(this.mainMappingStrategy.getShadowColumnsForInsert());
	}
	
	public Collection<ShadowColumnValueProvider<C, Object, T>> getShadowColumnsForUpdate() {
		return Collections.unmodifiableCollection(this.mainMappingStrategy.getShadowColumnsForUpdate());
	}
	
	public void addShadowColumns(ClassMappingStrategy<C, I, T> classMappingStrategy) {
		classMappingStrategy.mainMappingStrategy.getShadowColumnsForInsert().forEach(this::addShadowColumnInsert);
		classMappingStrategy.mainMappingStrategy.getShadowColumnsForUpdate().forEach(this::addShadowColumnUpdate);
	}
	
	public void addVersionedColumn(ReversibleAccessor propertyAccessor, Column<T, Object> column) {
		this.versioningMapping.put(propertyAccessor, column);
	}
	
	@Override
	public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mainMappingStrategy.addShadowColumnInsert(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		insertableColumns.add((Column<T, Object>) valueProvider.getColumn());
	}
	
	@Override
	public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mainMappingStrategy.addShadowColumnUpdate(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		updatableColumns.add((Column<T, Object>) valueProvider.getColumn());
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint accessor) {
		mainMappingStrategy.addPropertySetByConstructor(accessor);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		mainMappingStrategy.addShadowColumnSelect(column);
		fillSelectableColumns();
	}
	
	/**
	 * Sets a particular strategy for a given property.
	 * The property type is supposed to be complex so it needs multiple columns for persistence and then needs an {@link EmbeddedBeanMappingStrategy}
	 * 
	 * @param property an object representing a {@link Field} or {@link Method}
	 * @param mappingStrategy the strategy that should be used to persist the member
	 */
	public <O> void put(ReversibleAccessor<C, O> property, EmbeddedBeanMappingStrategy<O, T> mappingStrategy) {
		mappingStrategies.put((ReversibleAccessor) property, (EmbeddedBeanMappingStrategy) mappingStrategy);
		// update columns lists
		addInsertableColumns(mappingStrategy);
		addUpdatableColumns(mappingStrategy);
		addSelectableColumns(mappingStrategy);
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Map<Column<T, Object>, Object> insertValues = mainMappingStrategy.getInsertValues(c);
		getVersionedKeyValues(c).entrySet().stream()
				// autoincrement columns mustn't be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue = fieldStrategyEntry.getKey().get(c);
			Map<Column<T, Object>, Object> fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		}
		return insertValues;
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<UpwhereColumn<T>, Object> toReturn;
		if (modified != null && unmodified != null && !getId(modified).equals(getId(unmodified))) {
			// entities are different, so there's no value to be updated 
			toReturn = new HashMap<>();
		} else {
			toReturn = mainMappingStrategy.getUpdateValues(modified, unmodified, allColumns);
			for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
				ReversibleAccessor<C, Object> accessor = fieldStrategyEntry.getKey();
				Object modifiedValue = accessor.get(modified);
				Object unmodifiedValue = unmodified == null ? null : accessor.get(unmodified);
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
				// Determining on which instance we should take where values : unmodified by default, modified for rough update (unmodified is null)
				C whereSource = unmodified != null ? unmodified : modified;
				for (Entry<Column<T, Object>, Object> entry : getVersionedKeyValues(whereSource).entrySet()) {
					toReturn.put(new UpwhereColumn<>(entry.getKey(), false), entry.getValue());
				}
			}
		}
		return toReturn;
	}
	
	/**
	 * Build columns that can be inserted: columns minus generated keys
	 */
	private void fillInsertableColumns() {
		insertableColumns.clear();
		addInsertableColumns(mainMappingStrategy);
		mappingStrategies.values().forEach(this::addInsertableColumns);
		insertableColumns.addAll(versioningMapping.values());
		// NB : generated keys are never inserted but left because DMLGenerator needs its presence to detect it
		// and also it prevents to generate not empty statement when they are Alone in the Dark ;) 
	}
	
	private void addInsertableColumns(EmbeddedBeanMappingStrategy<?, T> embeddedBeanMapping) {
		insertableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	/**
	 * Build columns that can be updated: columns minus keys
	 */
	private void fillUpdatableColumns() {
		updatableColumns.clear();
		addUpdatableColumns(mainMappingStrategy);
		mappingStrategies.values().forEach(this::addUpdatableColumns);
		updatableColumns.addAll(versioningMapping.values());
		// keys are never updated
		updatableColumns.removeAll(getTargetTable().getPrimaryKey().getColumns());
		updatableColumns.removeIf(Column::isAutoGenerated);
	}
	
	private void addUpdatableColumns(EmbeddedBeanMappingStrategy<?, T> embeddedBeanMapping) {
		updatableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	private void fillSelectableColumns() {
		selectableColumns.clear();
		addSelectableColumns(mainMappingStrategy);
		selectableColumns.addAll(getTargetTable().getPrimaryKey().getColumns());
		selectableColumns.addAll(versioningMapping.values());
		mappingStrategies.values().forEach(this::addSelectableColumns);
	}
	
	private void addSelectableColumns(EmbeddedBeanMappingStrategy<?, T> embeddedBeanMapping) {
		selectableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	@Override
	public Map<Column<T, Object>, Object> getVersionedKeyValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		toReturn.putAll(getIdMappingStrategy().getIdentifierAssembler().getColumnValues(getId(c)));
		toReturn.putAll(getVersionedColumnsValues(c));
		return toReturn;
	}
	
	private Map<Column<T, Object>, Object> getVersionedColumnsValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		for (Entry<ReversibleAccessor, Column<T, Object>> columnEntry : versioningMapping.entrySet()) {
			toReturn.put(columnEntry.getValue(), columnEntry.getKey().get(c));
		}
		return toReturn;
	}
	
	@Override
	public Iterable<Column<T, Object>> getVersionedKeys() {
		HashSet<Column<T, Object>> columns = new HashSet<>(versioningMapping.values());
		Set<Column<T, Object>> keyColumns = getTargetTable().getPrimaryKey().getColumns();
		columns.addAll(keyColumns);
		return Collections.unmodifiableSet(columns);
	}
	
	@Override
	public I getId(C c) {
		return getIdMappingStrategy().getIdAccessor().getId(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		getIdMappingStrategy().getIdAccessor().setId(c, identifier);
	}
	
	@Override
	public boolean isNew(C c) {
		return getIdMappingStrategy().isNew(c);
	}
	
	@Override
	public C transform(Row row) {
		C toReturn = getRowTransformer().transform(row);
		// fixing identifier
		// Note : this may be done twice in single column primary key case, because constructor expects that the column must be present in the
		// mapping, then it is used by the SimpleIdentifierAssembler
		ColumnedRow columnedRow = new ColumnedRow();
		setId(toReturn, getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow));
		// filling other properties
		for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> mappingStrategyEntry : mappingStrategies.entrySet()) {
			mappingStrategyEntry.getKey().toMutator().set(toReturn, mappingStrategyEntry.getValue().transform(row));
		}
		return toReturn;
	}
	
	@Override
	public ToBeanRowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return getRowTransformer().copyWithAliases(columnedRow);
	}
	
	public ToBeanRowTransformer<C> getRowTransformer() {
		return mainMappingStrategy.getRowTransformer();
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		getRowTransformer().addTransformerListener(listener);
	}
}
