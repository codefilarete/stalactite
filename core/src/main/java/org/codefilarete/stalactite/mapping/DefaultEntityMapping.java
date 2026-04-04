package org.codefilarete.stalactite.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
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
 * <li>a main strategy : an embedded one ({@link EmbeddedClassMapping}, accessible by {@link #getMapping()}</li>
 * <li>an id strategy : {@link SimpleIdMapping} accessible with {@link #getIdMapping()}</li>
 * <li>optional version mapping : accessible with {@link #getVersionedKeys()} for instance</li>
 * </ul>
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
	
	private final EmbeddedClassMapping<C, T> mapping;
	
	private final Set<Column<T, ?>> insertableColumns = new KeepOrderSet<>();
	
	private final Set<Column<T, ?>> updatableColumns = new KeepOrderSet<>();
	
	private final Set<Column<T, ?>> selectableColumns = new KeepOrderSet<>();
	
	private final IdMapping<C, I> idMapping;
	
	@Nullable
	private final Duo<ReadWritePropertyAccessPoint<C, ?>, Column<T, ?>> versioningMapping;
	
	private final boolean identifierSetByBeanFactory;
	
	/**
	 * Main constructor
	 * Oriented for single column identifier / primary key. Prefer {@link #DefaultEntityMapping(Class, Table, Map, IdMapping)} for composed id.
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param identifierProperty identifier of the persisted class
	 * @param identifierInsertionManager manager of identifiers
	 */
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReadWritePropertyAccessPoint<C, ?>, ? extends Column<T, ?>> propertyToColumn,
								ReadWritePropertyAccessPoint<C, I> identifierProperty,
								IdentifierInsertionManager<C, I> identifierInsertionManager) {
		if (identifierProperty == null) {
			throw new UnsupportedOperationException("No identifier property for " + Reflections.toString(classToPersist));
		}
		if (targetTable.getPrimaryKey() == null) {
			throw new UnsupportedOperationException("No primary key column defined for " + targetTable.getAbsoluteName());
		}
		this.mapping = new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn);
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
	 *
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn mapping between bean "properties" and table columns
	 * @param idMapping mapping strategy of class identifier
	 */
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReadWritePropertyAccessPoint<C, ?>, Column<T, ?>> propertyToColumn,
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
	 */
	public DefaultEntityMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReadWritePropertyAccessPoint<C, ?>, ? extends Column<T, ?>> propertyToColumn,
								Map<? extends PropertyMutator<C, ?>, ? extends Column<T, ?>> readonlyColumns,
								@Nullable Duo<? extends ReadWritePropertyAccessPoint<C, ?>, ? extends Column<T, ?>> versioningMapping,
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
		Map<PropertyMutator<C, ?>, Column<T, ?>> readonlyPropertiesMapping = new KeepOrderMap<>(readonlyColumns);
		if (versioningMapping != null) {
			readonlyPropertiesMapping.put(versioningMapping.getLeft(), versioningMapping.getRight());
		}
		this.mapping = new EmbeddedClassMapping<>(classToPersist, targetTable, propertyToColumn, readonlyPropertiesMapping, entityFactory);
		this.idMapping = idMapping;
		this.identifierSetByBeanFactory = identifierSetByBeanFactory;
		this.versioningMapping = (Duo<ReadWritePropertyAccessPoint<C, ?>, Column<T, ?>>) versioningMapping;
		fillInsertableColumns();
		fillUpdatableColumns();
		fillSelectableColumns();
	}
	
	public Class<C> getClassToPersist() {
		return mapping.getClassToPersist();
	}
	
	@Override
	public T getTargetTable() {
		return mapping.getTargetTable();
	}
	
	public EmbeddedClassMapping<C, T> getMapping() {
		return mapping;
	}
	
	/**
	 * Implementation which returns all properties mapping, even embedded ones.
	 * The Result is built dynamically.
	 *
	 * @return all properties mapping, even embedded ones
	 */
	@Override
	public Map<ReadWritePropertyAccessPoint<C, ?>, Column<T, ?>> getPropertyToColumn() {
		return new KeepOrderMap<>(getMapping().getPropertyToColumn());
	}
	
	@Override
	public Map<PropertyMutator<C, ?>, Column<T, ?>> getReadonlyPropertyToColumn() {
		return new KeepOrderMap<>(getMapping().getReadonlyPropertyToColumn());
	}
	
	@Override
	@Nullable
	public Duo<ReadWritePropertyAccessPoint<C, ?>, Column<T, ?>> getVersioningMapping() {
		return versioningMapping;
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> getReadConverters() {
		return mapping.getReadConverters();
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> getWriteConverters() {
		return mapping.getWriteConverters();
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
		return Collections.unmodifiableCollection(this.mapping.getShadowColumnsForInsert());
	}
	
	public Collection<ShadowColumnValueProvider<C, T>> getShadowColumnsForUpdate() {
		return Collections.unmodifiableCollection(this.mapping.getShadowColumnsForUpdate());
	}
	
	public void addShadowColumns(DefaultEntityMapping<C, I, T> entityMapping) {
		entityMapping.mapping.getShadowColumnsForInsert().forEach(this::addShadowColumnInsert);
		entityMapping.mapping.getShadowColumnsForUpdate().forEach(this::addShadowColumnUpdate);
	}
	
	@Override
	public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mapping.addShadowColumnInsert(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		insertableColumns.addAll(valueProvider.getColumns());
	}
	
	@Override
	public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		// we delegate value computation to the default mapping strategy
		mapping.addShadowColumnUpdate(valueProvider);
		// we must register it as an insertable column so we'll generate the right SQL order
		updatableColumns.addAll(valueProvider.getColumns());
	}
	
	@Override
	public void addPropertySetByConstructor(PropertyAccessPoint<C, ?> accessor) {
		mapping.addPropertySetByConstructor(accessor);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		mapping.addShadowColumnSelect(column);
		fillSelectableColumns();
	}
	
	@Override
	public Map<Column<T, ?>, ?> getInsertValues(C c) {
		Map<Column<T, ?>, Object> insertValues = mapping.getInsertValues(c);
		getVersionedKeyValues(c).entrySet().stream()
				// autoincrement columns mustn't be written
				.filter(entry -> !entry.getKey().isAutoGenerated())
				.forEach(entry -> insertValues.put(entry.getKey(), entry.getValue()));
		return insertValues;
	}
	
	@Override
	public Map<UpwhereColumn<T>, ?> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<UpwhereColumn<T>, Object> toReturn;
		if (modified != null && unmodified != null && !getId(modified).equals(getId(unmodified))) {
			// entities are different, so there's no value to be updated 
			toReturn = new HashMap<>();
		} else {
			toReturn = mapping.getUpdateValues(modified, unmodified, allColumns);
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
	 * Build columns that can be inserted: columns minus generated keys
	 */
	private void fillInsertableColumns() {
		insertableColumns.clear();
		addInsertableColumns(mapping);
		insertableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
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
		addUpdatableColumns(mapping);
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
		addSelectableColumns(mapping);
		selectableColumns.addAll(getIdMapping().<T>getIdentifierAssembler().getColumns());
		if (versioningMapping != null) {
			selectableColumns.add(versioningMapping.getRight());
		}
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
				C toReturn = mapping.getRowTransformer().transform(row);
				// fixing identifier
				if (!identifierSetByBeanFactory) {
					setId(toReturn, getIdMapping().getIdentifierAssembler().assemble(row));
				}
				return toReturn;
			}

			@Override
			public C newBeanInstance(ColumnedRow row) {
				return mapping.transform(row);
			}

			@Override
			public void applyRowToBean(ColumnedRow row, C bean) {
				mapping.getRowTransformer().applyRowToBean(row, bean);
			}

			@Override
			public void addTransformerListener(TransformerListener<? extends C> listener) {
				mapping.addTransformerListener((TransformerListener<C>) listener);
			}
		};
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		getRowTransformer().addTransformerListener(listener);
	}
}
