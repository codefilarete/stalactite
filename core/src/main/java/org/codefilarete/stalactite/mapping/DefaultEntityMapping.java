package org.codefilarete.stalactite.mapping;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;

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
public class DefaultEntityMapping<C, I, T extends Table<T>> implements EntityMapping<C, I, T> {
	
	private final EmbeddedClassMapping<C, T> mainMapping;
	
	private final Set<Column<T, ?>> insertableColumns = new KeepOrderSet<>();
	
	private final Set<Column<T, ?>> updatableColumns = new KeepOrderSet<>();
	
	private final Set<Column<T, ?>> selectableColumns = new KeepOrderSet<>();
	
	private final Map<ReversibleAccessor<C, ?>, EmbeddedBeanMapping<?, T>> embeddedMappings = new KeepOrderMap<>();
	
	private final IdMapping<C, I> idMapping;
	
	@Nullable
	private final Duo<ReversibleAccessor<C, ?>, Column<T, ?>> versioningMapping;
	
	private final boolean identifierSetByBeanFactory;
	
	/**
	 * Main constructor
	 * Oriented for single column identifier / primary key. Prefer {@link #DefaultEntityMapping(Class, Table, Map, IdMapping)} for composed id.
	 * It only defines main class and table, secondary ones, such as embedded class, must be defined through {@link #put(ReversibleAccessor, EmbeddedBeanMapping)}
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param identifierProperty identifier of the persisted class
	 * @param identifierInsertionManager manager of identifiers
	 * @see #put(ReversibleAccessor, EmbeddedBeanMapping) 
	 */
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> propertyToColumn,
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
		this.idMapping = new SimpleIdMapping<>(identifierProperty, identifierInsertionManager, new SingleIdentifierAssembler<>(identifierColumn));
		this.identifierSetByBeanFactory = false;
		this.versioningMapping = null;
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
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, ?>, Column<T, ?>> propertyToColumn,
								IdMapping<C, I> idMapping) {
		this(classToPersist, targetTable, propertyToColumn, Collections.emptyMap(), null, idMapping, null, false);
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
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> propertyToColumn,
								Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> readonlyColumns,
								@Nullable Duo<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> versioningMapping,
								IdMapping<C, I> idMapping,
								@Nullable Function<ColumnedRow, C> entityFactory,
								boolean identifierSetByBeanFactory) {
		if (idMapping.getIdAccessor() == null) {
			throw new UnsupportedOperationException("No identifier property defined for " + Reflections.toString(classToPersist));
		}
		if (targetTable.getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + targetTable.getAbsoluteName());
		}
		
		// adding versioning columns to properties for reading
		Map<ReversibleAccessor<C, ?>, Column<T, ?>> readonlyPropertiesMapping = new KeepOrderMap<>(readonlyColumns);
		if (versioningMapping != null) {
			readonlyPropertiesMapping.put(versioningMapping.getLeft(), versioningMapping.getRight());
		}
		this.mainMapping = new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn, readonlyPropertiesMapping, entityFactory);
		this.idMapping = idMapping;
		this.identifierSetByBeanFactory = identifierSetByBeanFactory;
		this.versioningMapping = (Duo<ReversibleAccessor<C, ?>, Column<T, ?>>) versioningMapping;
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
	public Map<ReversibleAccessor<C, ?>, EmbeddedBeanMapping<?, T>> getEmbeddedBeanStrategies() {
		return embeddedMappings;
	}
	
	/**
	 * Implementation which returns all properties mapping, even embedded ones.
	 * The Result is built dynamically.
	 * 
	 * @return all properties mapping, even embedded ones
	 */
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getPropertyToColumn() {
		Map<ReversibleAccessor<C, ?>, Column<T, ?>> result = new KeepOrderMap<>();
		result.putAll(getMainMapping().getPropertyToColumn());
		for (Entry<ReversibleAccessor<C, ?>, EmbeddedBeanMapping<?, T>> value : embeddedMappings.entrySet()) {
			value.getValue().getPropertyToColumn().forEach((k, v) -> result.put(new AccessorChain<>(value.getKey(), k), v));
		}
		return result;
	}
	
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getReadonlyPropertyToColumn() {
		Map<ReversibleAccessor<C, ?>, Column<T, ?>> result = new KeepOrderMap<>();
		result.putAll(getMainMapping().getReadonlyPropertyToColumn());
		for (Entry<ReversibleAccessor<C, ?>, EmbeddedBeanMapping<?, T>> value : embeddedMappings.entrySet()) {
			value.getValue().getReadonlyPropertyToColumn().forEach((k, v) -> {
				// code below is only to ensure that cast as Accessor will be fine in all circumstances
				Accessor<?, ?> kAsAccessor;
				if (k instanceof ReversibleMutator) {
					kAsAccessor = ((ReversibleMutator<Object, Object>) k).toAccessor();
				} else {
					// This is not an absolute end : it would be sufficient if we could build an AccessorChain with last element
					// being a Mutator, making created AccessorChain a Mutator (non reversible in our case) this should be implemented to avoid this exception
					throw new UnsupportedOperationException("Given accessor " + AccessorDefinition.toString(k) + " can't be converted to an Accessor"
							+ " to make it last element of Accessor chain");
				}
				result.put(new AccessorChain<C, Object>(value.getKey(), kAsAccessor).toMutator(), v);
			});
		}
		return result;
	}
	
	@Override
	@Nullable
	public Duo<ReversibleAccessor<C, ?>, Column<T, ?>> getVersioningMapping() {
		return versioningMapping;
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
		return mainMapping.getReadConverters();
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>> getWriteConverters() {
		return mainMapping.getWriteConverters();
	}
	
	/**
	 * Gives columns that can be inserted: columns minus generated keys
	 * @return columns of all mapping strategies without auto-generated keys, as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, ?>> getInsertableColumns() {
		return Collections.unmodifiableSet(insertableColumns);
	}
	
	/**
	 * Gives columns that can be updated: columns minus keys
	 * @return columns of all mapping strategies without getKey(), as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, ?>> getUpdatableColumns() {
		return Collections.unmodifiableSet(updatableColumns);
	}
	
	/**
	 * Gives columns that can be selected
	 * @return columns of all mapping strategies, as an immutable {@link Set}
	 */
	@Override
	public Set<Column<T, ?>> getSelectableColumns() {
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
	
	public void addShadowColumns(DefaultEntityMapping<C, I, T> entityMapping) {
		entityMapping.mainMapping.getShadowColumnsForInsert().forEach(this::addShadowColumnInsert);
		entityMapping.mainMapping.getShadowColumnsForUpdate().forEach(this::addShadowColumnUpdate);
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
		embeddedMappings.put(property, mappingStrategy);
		// update columns lists
		addInsertableColumns(mappingStrategy);
		addUpdatableColumns(mappingStrategy);
		addSelectableColumns(mappingStrategy);
	}
	
	@Override
	public Map<Column<T, ?>, ?> getInsertValues(C c) {
		Map<Column<T, ?>, Object> insertValues = mainMapping.getInsertValues(c);
		getVersionedKeyValues(c).entrySet().stream()
				// autoincrement columns mustn't be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		this.foreachMappedField(mappingEntry -> {
			Object fieldValue = mappingEntry.getKey().get(c);
			Map<Column<T, ?>, ?> fieldInsertValues = mappingEntry.getValue().getInsertValues(fieldValue);
			insertValues.putAll(fieldInsertValues);
		});
		return insertValues;
	}
	
	@Override
	public Map<UpwhereColumn<T>, ?> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<UpwhereColumn<T>, Object> toReturn;
		if (modified != null && unmodified != null && !getId(modified).equals(getId(unmodified))) {
			// entities are different, so there's no value to be updated 
			toReturn = new HashMap<>();
		} else {
			toReturn = mainMapping.getUpdateValues(modified, unmodified, allColumns);
			this.foreachMappedField(mappingEntry -> {
				ReversibleAccessor<C, ?> accessor = mappingEntry.getKey();
				Object modifiedValue = accessor.get(modified);
				Object unmodifiedValue = unmodified == null ? null : accessor.get(unmodified);
				Map<UpwhereColumn<T>, ?> fieldUpdateValues = mappingEntry.getValue().getUpdateValues(modifiedValue, unmodifiedValue, allColumns);
				toReturn.putAll(fieldUpdateValues);
			});
			if (!toReturn.isEmpty()) {
				if (allColumns) {
					Set<Column<T, ?>> missingColumns = new HashSet<>(getUpdatableColumns());
					missingColumns.removeAll(UpwhereColumn.getUpdateColumns(toReturn).keySet());
					for (Column<T, ?> missingColumn : missingColumns) {
						toReturn.put(new UpwhereColumn<>(missingColumn, true), null);
					}
				}
				// Determining on which instance we should take where values : unmodified by default, modified for rough update (unmodified is null)
				C whereSource = unmodified != null ? unmodified : modified;
				for (Entry<Column<T, ?>, ?> entry : getVersionedKeyValues(whereSource).entrySet()) {
					toReturn.put(new UpwhereColumn<>(entry.getKey(), false), entry.getValue());
				}
			}
		}
		return toReturn;
	}
	
	/**
	 * Iterates over embeddedMappings field of this instance and call the given consumer for each of them.
	 * Made to deal with generics problems on caller (without it, code doesn't compile due to "?")
	 * 
	 * @param consumer code that consumes each embeddedMappings entry
	 * @param <E> type of mapped beans 
	 */
	private <E> void foreachMappedField(Consumer<Entry<ReversibleAccessor<C, E>, EmbeddedBeanMapping<E, T>>> consumer) {
		embeddedMappings.entrySet().forEach((Consumer) consumer);
	}
	
	/**
	 * Build columns that can be inserted: columns minus generated keys
	 */
	private void fillInsertableColumns() {
		insertableColumns.clear();
		addInsertableColumns(mainMapping);
		insertableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
		embeddedMappings.values().forEach(this::addInsertableColumns);
		if (versioningMapping != null) {
			insertableColumns.add(versioningMapping.getRight());
		}
		// NB : generated keys are never inserted but left because DMLGenerator needs its presence to detect it
		// and also it prevents to generate empty statement when they are Alone in the Dark ;)
	}
	
	private void addInsertableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		insertableColumns.addAll(embeddedBeanMapping.getWritableColumns());
	}
	
	/**
	 * Build columns that can be updated: columns minus keys
	 */
	private void fillUpdatableColumns() {
		updatableColumns.clear();
		addUpdatableColumns(mainMapping);
		embeddedMappings.values().forEach(this::addUpdatableColumns);
		if (versioningMapping != null) {
			updatableColumns.add(versioningMapping.getRight());
		}
		// keys are never updated
		updatableColumns.removeAll(getTargetTable().getPrimaryKey().getColumns());
		updatableColumns.removeIf(Column::isAutoGenerated);
	}
	
	private void addUpdatableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		updatableColumns.addAll(embeddedBeanMapping.getWritableColumns());
	}
	
	private void fillSelectableColumns() {
		selectableColumns.clear();
		addSelectableColumns(mainMapping);
		selectableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
		if (versioningMapping != null) {
			selectableColumns.add(versioningMapping.getRight());
		}
		embeddedMappings.values().forEach(this::addSelectableColumns);
	}
	
	private void addSelectableColumns(EmbeddedBeanMapping<?, T> embeddedBeanMapping) {
		selectableColumns.addAll(embeddedBeanMapping.getColumns());
	}
	
	@Override
	public Map<Column<T, ?>, ?> getVersionedKeyValues(C c) {
		Map<Column<T, ?>, Object> toReturn = new HashMap<>();
		toReturn.putAll(getIdMapping().<T>getIdentifierAssembler().getColumnValues(getId(c)));
		if (versioningMapping != null) {
			toReturn.put(versioningMapping.getRight(), versioningMapping.getLeft().get(c));
		}
		return toReturn;
	}
	
	@Override
	public Iterable<Column<T, ?>> getVersionedKeys() {
		// we return a stable set to keep tests stable, shouldn't impact performances
		Set<Column<T, ?>> columns = new KeepOrderSet<>();
		if (versioningMapping != null) {
			columns.add(versioningMapping.getRight());
		}
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
	public C transform(ColumnedRow row) {
		return getRowTransformer().transform(row);
	}
	
	@Override
	public RowTransformer<C> getRowTransformer() {
		return new RowTransformer<C>() {
			@Override
			public C transform(ColumnedRow row) {
				C toReturn = mainMapping.getRowTransformer().transform(row);
				// fixing identifier
				if (!identifierSetByBeanFactory) {
					setId(toReturn, getIdMapping().getIdentifierAssembler().assemble(row));
				}
				// filling other properties
				foreachMappedField(mappingEntry -> {
					mappingEntry.getKey().toMutator().set(toReturn, mappingEntry.getValue().transform(row));
				});
				return toReturn;
			}

			@Override
			public C newBeanInstance(ColumnedRow row) {
				return mainMapping.transform(row);
			}

			@Override
			public void applyRowToBean(ColumnedRow row, C bean) {
				mainMapping.getRowTransformer().applyRowToBean(row, bean);
			}

			@Override
			public void addTransformerListener(TransformerListener<? extends C> listener) {
				mainMapping.addTransformerListener((TransformerListener<C>) listener);
			}
		};
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		getRowTransformer().addTransformerListener(listener);
	}
}
