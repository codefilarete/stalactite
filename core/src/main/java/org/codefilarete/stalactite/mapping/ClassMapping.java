package org.codefilarete.stalactite.mapping;

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

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Reflections;

/**
 * <p>
 * Main class for persistence entity mapping description.
 * Composed of:
 * <ul>
 * <li>a main strategy : an embedded one ({@link EmbeddedClassMapping}, accessible by {@link #getMainMapping()}</li>
 * <li>an id strategy : {@link SimpleIdMapping} accessible with {@link #getIdMapping()}</li>
 * <li>optional version mapping : accessible with {@link #getVersionedKeys()} for instance</li>
 * <li>additional mappings (for embeddable for instance) : see {@link #put(ReversibleAccessor, EmbeddedBeanMapping)}</li>
 * </ul>
 * </p>
 * <p>
 * Note that mapping is defined through getter ({@link ReversibleAccessor}) and not setter ({@link ReversibleMutator}) only
 * because a choice must be done between them, else the concrete class {@link PropertyAccessor} is the choice, which is less open.
 * </p>
 *
 * <p>
 * Mapping definition can be eased thanks to {@link PersistentFieldHarvester} or {@link Accessors}
 * </p>
 * 
 * <br/>
 * <b>THIS CLASS DOESN'T ADDRESS RELATION MAPPING</b>, because it's not the purpose of Stalactite 'core' module, see 'orm' module.
 * Meanwhile one can use {@link org.codefilarete.stalactite.query.model.Query} to construct complex type.
 * 
 * @author Guillaume Mary
 * @see org.codefilarete.stalactite.query.model.Query
 */
public class ClassMapping<C, I, T extends Table<T>> implements EntityMapping<C, I, T> {
	
	private final EmbeddedClassMapping<C, T> mainMapping;
	
	private final Set<Column<T, Object>> insertableColumns = new LinkedHashSet<>();
	
	private final Set<Column<T, Object>> updatableColumns = new LinkedHashSet<>();
	
	private final Set<Column<T, Object>> selectableColumns = new LinkedHashSet<>();
	
	private final Map<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> mappingStrategies = new HashMap<>();
	
	private final IdMapping<C, I> idMapping;
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> versioningMapping = new HashMap<>();
	
	private final boolean identifierSetByBeanFactory;
	
	/**
	 * Main constructor
	 * Oriented for single column identifier / primary key. Prefer {@link #ClassMapping(Class, Table, Map, IdMapping)} for composed id.
	 * It only defines main class and table, secondary ones, such as embedded class, must be defined through {@link #put(ReversibleAccessor, EmbeddedBeanMapping)}
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param identifierProperty identifier of the persisted class
	 * @param identifierInsertionManager manager of identifiers
	 * @see #put(ReversibleAccessor, EmbeddedBeanMapping) 
	 */
	public ClassMapping(Class<C> classToPersist,
						T targetTable,
						Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> propertyToColumn,
						ReversibleAccessor<C, I> identifierProperty,
						IdentifierInsertionManager<C, I> identifierInsertionManager) {
		if (identifierProperty == null) {
			throw new UnsupportedOperationException("No identifier property for " + Reflections.toString(classToPersist));
		}
		if (targetTable.getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + targetTable.getAbsoluteName());
		}
		this.mainMapping = new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn);
		// identifierAccessor must be the same instance as those stored in propertyToColumn for Map.remove method used in foreach()
		Column<T, I> identifierColumn = (Column<T, I>) propertyToColumn.get(identifierProperty);
		if (identifierColumn == null) {
			throw new IllegalArgumentException("Bean identifier '" + AccessorDefinition.toString(identifierProperty) + "' must have its matching column in the mapping");
		}
		if (!identifierColumn.isPrimaryKey()) {
			throw new UnsupportedOperationException("Accessor '" + AccessorDefinition.toString(identifierProperty)
					+ "' is declared as identifier but mapped column " + identifierColumn + " is not the primary key of table");
		}
		this.idMapping = new SimpleIdMapping<>(identifierProperty, identifierInsertionManager, new SimpleIdentifierAssembler<>(identifierColumn));
		this.identifierSetByBeanFactory = false;
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
	}
	
	/**
	 * Secondary constructor, for composed id because one can precisely define the {@link IdMapping} by giving a {@link ComposedIdMapping}
	 * for instance.
	 * It only defines main class and table, secondary ones, such as embedded class, must be defined through {@link #put(ReversibleAccessor, EmbeddedBeanMapping)}
	 *
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param idMapping mapping strategy of class identifier
	 * @see #put(ReversibleAccessor, EmbeddedBeanMapping)
	 */
	public ClassMapping(Class<C> classToPersist,
						T targetTable,
						Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
						IdMapping<C, I> idMapping) {
		this(new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn), idMapping, false);
	}
	
	/**
	 * Constructor that let caller give a bean factory. The bean factory is given a value provider of the form of a {@link Function&lt;Column, Object&gt;}
	 * that can be called to get values to build expected entity or determine the right type to be instantiated according to current row values.
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param idMapping mapping strategy of class identifier
	 * @param entityFactory entity factory
	 * @see #put(ReversibleAccessor, EmbeddedBeanMapping)
	 */
	public ClassMapping(Class<C> classToPersist,
						T targetTable,
						Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> propertyToColumn,
						IdMapping<C, I> idMapping,
						Function<Function<Column<?, ?>, Object>, C> entityFactory,
						boolean identifierSetByBeanFactory) {
		this(new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn, entityFactory), idMapping, identifierSetByBeanFactory);
	}
	
	private ClassMapping(EmbeddedClassMapping<C, T> mainMapping, IdMapping<C, I> idMapping, boolean identifierSetByBeanFactory) {
		if (idMapping.getIdAccessor() == null) {
			throw new UnsupportedOperationException("No identifier property defined for " + Reflections.toString(mainMapping.getClassToPersist()));
		}
		if (mainMapping.getTargetTable().getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + mainMapping.getTargetTable().getAbsoluteName());
		}
		this.mainMapping = mainMapping;
		this.idMapping = idMapping;
		this.identifierSetByBeanFactory = identifierSetByBeanFactory;
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
	}
	
	public Class<C> getClassToPersist() {
		return mainMapping.getClassToPersist();
	}
	
	@Override
	public T getTargetTable() {
		return mainMapping.getTargetTable();
	}
	
	public EmbeddedClassMapping<C, T> getMainMapping() {
		return mainMapping;
	}
	
	@Override
	public Map<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> getEmbeddedBeanStrategies() {
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
		result.putAll(getMainMapping().getPropertyToColumn());
		for (Entry<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> value : mappingStrategies.entrySet()) {
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
	public IdMapping<C, I> getIdMapping() {
		return idMapping;
	}
	
	public Collection<ShadowColumnValueProvider<C, T>> getShadowColumnsForInsert() {
		return Collections.unmodifiableCollection(this.mainMapping.getShadowColumnsForInsert());
	}
	
	public Collection<ShadowColumnValueProvider<C, T>> getShadowColumnsForUpdate() {
		return Collections.unmodifiableCollection(this.mainMapping.getShadowColumnsForUpdate());
	}
	
	public void addShadowColumns(ClassMapping<C, I, T> classMappingStrategy) {
		classMappingStrategy.mainMapping.getShadowColumnsForInsert().forEach(this::addShadowColumnInsert);
		classMappingStrategy.mainMapping.getShadowColumnsForUpdate().forEach(this::addShadowColumnUpdate);
	}
	
	public void addVersionedColumn(ReversibleAccessor propertyAccessor, Column<T, Object> column) {
		this.versioningMapping.put(propertyAccessor, column);
	}
	
	@Override
	public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mainMapping.addShadowColumnInsert(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		insertableColumns.addAll(valueProvider.getColumns());
	}
	
	@Override
	public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mainMapping.addShadowColumnUpdate(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		updatableColumns.addAll(valueProvider.getColumns());
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint<C> accessor) {
		mainMapping.addPropertySetByConstructor(accessor);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		mainMapping.addShadowColumnSelect(column);
		fillSelectableColumns();
	}
	
	/**
	 * Sets a particular strategy for a given property.
	 * The property type is supposed to be complex so it needs multiple columns for persistence and then needs an {@link EmbeddedBeanMapping}
	 * 
	 * @param property an object representing a {@link Field} or {@link Method}
	 * @param mappingStrategy the strategy that should be used to persist the member
	 */
	public <O> void put(ReversibleAccessor<C, O> property, EmbeddedBeanMapping<O, T> mappingStrategy) {
		mappingStrategies.put((ReversibleAccessor) property, (EmbeddedBeanMapping) mappingStrategy);
		// update columns lists
		addInsertableColumns(mappingStrategy);
		addUpdatableColumns(mappingStrategy);
		addSelectableColumns(mappingStrategy);
	}
	
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Map<Column<T, Object>, Object> insertValues = mainMapping.getInsertValues(c);
		getVersionedKeyValues(c).entrySet().stream()
				// autoincrement columns mustn't be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
			Object fieldValue = fieldStrategyEntry.getKey().get(c);
			Map<Column<T, Object>, Object> fieldInsertValues = fieldStrategyEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		}
		return insertValues;
	}
	
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<UpwhereColumn<T>, Object> toReturn;
		if (modified != null && unmodified != null && !getId(modified).equals(getId(unmodified))) {
			// entities are different, so there's no value to be updated 
			toReturn = new HashMap<>();
		} else {
			toReturn = mainMapping.getUpdateValues(modified, unmodified, allColumns);
			for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> fieldStrategyEntry : mappingStrategies.entrySet()) {
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
		addInsertableColumns(mainMapping);
		insertableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
		mappingStrategies.values().forEach(this::addInsertableColumns);
		insertableColumns.addAll(versioningMapping.values());
		// NB : generated keys are never inserted but left because DMLGenerator needs its presence to detect it
		// and also it prevents to generate not empty statement when they are Alone in the Dark ;) 
	}
	
	private void addInsertableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		insertableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	/**
	 * Build columns that can be updated: columns minus keys
	 */
	private void fillUpdatableColumns() {
		updatableColumns.clear();
		addUpdatableColumns(mainMapping);
		mappingStrategies.values().forEach(this::addUpdatableColumns);
		updatableColumns.addAll(versioningMapping.values());
		// keys are never updated
		updatableColumns.removeAll(getTargetTable().getPrimaryKey().getColumns());
		updatableColumns.removeIf(Column::isAutoGenerated);
	}
	
	private void addUpdatableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		updatableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	private void fillSelectableColumns() {
		selectableColumns.clear();
		addSelectableColumns(mainMapping);
		selectableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
		selectableColumns.addAll(versioningMapping.values());
		mappingStrategies.values().forEach(this::addSelectableColumns);
	}
	
	private void addSelectableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		selectableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	@Override
	public Map<Column<T, Object>, Object> getVersionedKeyValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		toReturn.putAll(getIdMapping().<T>getIdentifierAssembler().getColumnValues(getId(c)));
		toReturn.putAll(getVersionedColumnsValues(c));
		return toReturn;
	}
	
	private Map<Column<T, Object>, Object> getVersionedColumnsValues(C c) {
		Map<Column<T, Object>, Object> toReturn = new HashMap<>();
		for (Entry<ReversibleAccessor<C, Object>, Column<T, Object>> columnEntry : versioningMapping.entrySet()) {
			toReturn.put(columnEntry.getValue(), columnEntry.getKey().get(c));
		}
		return toReturn;
	}
	
	@Override
	public Iterable<Column<T, Object>> getVersionedKeys() {
		Set<Column<T, Object>> columns = new HashSet<>(versioningMapping.values());
		columns.addAll(getTargetTable().getPrimaryKey().getColumns());
		return Collections.unmodifiableSet(columns);
	}
	
	@Override
	public I getId(C c) {
		return getIdMapping().getIdAccessor().getId(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		getIdMapping().getIdAccessor().setId(c, identifier);
	}
	
	@Override
	public boolean isNew(C c) {
		return getIdMapping().isNew(c);
	}
	
	@Override
	public C transform(Row row) {
		C toReturn = getRowTransformer().transform(row);
		// fixing identifier
		// Note : this may be done twice in single column primary key case, because constructor expects that the column must be present in the
		// mapping, then it is used by the SimpleIdentifierAssembler
		ColumnedRow columnedRow = new ColumnedRow();
		if (!identifierSetByBeanFactory) {
			setId(toReturn, getIdMapping().getIdentifierAssembler().assemble(row, columnedRow));
		}
		// filling other properties
		for (Entry<? extends ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> mappingStrategyEntry : mappingStrategies.entrySet()) {
			mappingStrategyEntry.getKey().toMutator().set(toReturn, mappingStrategyEntry.getValue().transform(row));
		}
		return toReturn;
	}
	
	@Override
	public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		ToBeanRowTransformer<C> mainRowTransformer = getRowTransformer().copyWithAliases(columnedRow);
		return new RowTransformer<C>() {
			@Override
			public C transform(Row row) {
				C result = mainRowTransformer.transform(row);
				if (!identifierSetByBeanFactory) {
					setId(result, getIdMapping().getIdentifierAssembler().assemble(row, columnedRow));
				}
				return result;
			}
			
			@Override
			public void applyRowToBean(Row row, C bean) {
				mainRowTransformer.applyRowToBean(row, bean);
			}
			
			@Override
			public AbstractTransformer<C> copyWithAliases(ColumnedRow columnedRow) {
				// safeguard against unexpected behavior
				throw new IllegalStateException("copyWithAliases(..) is not expected to be called twice");
			}
			
			@Override
			public void addTransformerListener(TransformerListener<? extends C> listener) {
				mainRowTransformer.addTransformerListener(listener);
			}
		};
	}
	
	public ToBeanRowTransformer<C> getRowTransformer() {
		return mainMapping.getRowTransformer();
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		getRowTransformer().addTransformerListener(listener);
	}
}
