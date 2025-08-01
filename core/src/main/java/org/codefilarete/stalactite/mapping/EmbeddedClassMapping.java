package org.codefilarete.stalactite.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChainMutator;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Predicates;

/**
 * Persistence strategy for "embedded" bean (no identifier nor relation managed here) : straight mapping betwen some properties
 * of a class and some columns of a table.
 * 
 * @author Guillaume Mary
 */
public class EmbeddedClassMapping<C, T extends Table<T>> implements EmbeddedBeanMapping<C, T> {
	
	private final Class<C> classToPersist;
	
	private final T targetTable;
	
	private final Map<ReversibleAccessor<C, ?>, Column<T, ?>> propertyToColumn;
	
	// Could be a Map<Mutator....> if we could have a AccessorChain that can be a Mutator which is not currently the case
	private final Map<ReversibleAccessor<C, ?>, Column<T, ?>> readonlyPropertyToColumn;
	
	private final Set<Column<T, Object>> columns;
	
	/** Acts as a cache of insertable properties, could be dynamically deduced from {@link #propertyToColumn} */
	private final Map<Accessor<C, ?>, Column<T, ?>> insertableProperties;
	
	/** Acts as a cache of updatable properties, could be dynamically deduced from {@link #propertyToColumn} */
	private final Map<Accessor<C, ?>, Column<T, ?>> updatableProperties;
	
	private final ToBeanRowTransformer<C> rowTransformer;
	
	private DefaultValueDeterminer defaultValueDeterminer = new DefaultValueDeterminer() {};
	
	/**
	 * Columns and their value provider which are not officially mapped by a bean property.
	 * Those are for insertion time.
	 */
	private final KeepOrderSet<ShadowColumnValueProvider<C, T>> shadowColumnsForInsert = new KeepOrderSet<>();
	
	/**
	 * Columns and their value provider which are not officially mapped by a bean property.
	 * Those are for update time.
	 */
	private final KeepOrderSet<ShadowColumnValueProvider<C, T>> shadowColumnsForUpdate = new KeepOrderSet<>();
	
	private final ValueAccessPointSet<C> propertiesSetByConstructor = new ValueAccessPointSet<>();
	
	private ValueAccessPointMap<C, Converter<Object, Object>> readConverters = new ValueAccessPointMap<>();
	
	private ValueAccessPointMap<C, Converter<Object, Object>> writeConverters = new ValueAccessPointMap<>();
	
	/**
	 * Builds an embedded class mapping between its properties (as {@link ReversibleAccessor}) and some {@link Column}s.
	 * {@link Column}s are expected to be from same table, no strong control is made about that except generic type, caller must be aware of it.
	 * 
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn a mapping between Field and Column, expected to be coherent (fields of same class, column of same table)
	 */
	public EmbeddedClassMapping(Class<C> classToPersist, T targetTable, Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> propertyToColumn) {
		this(classToPersist, targetTable, propertyToColumn, new HashMap<>(), row -> Reflections.newInstance(classToPersist));
	}
	
	public EmbeddedClassMapping(Class<C> classToPersist,
								T targetTable,
								Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> propertiesMapping,
								Map<? extends ReversibleAccessor<C, ?>, ? extends Column<T, ?>> readonlyPropertiesMapping,
								Function<ColumnedRow, C> beanFactory) {
		this.classToPersist = classToPersist;
		this.targetTable = targetTable;
		this.propertyToColumn = new KeepOrderMap<>(propertiesMapping);
		
		// computing read columns
		this.readonlyPropertyToColumn = new KeepOrderMap<>(readonlyPropertiesMapping);
		Map<Column<T, ?>, Mutator> columnToField = Iterables.map(propertiesMapping.entrySet(), Entry::getValue, e -> e.getKey().toMutator(), KeepOrderMap::new);
		readonlyPropertiesMapping.forEach((accessor, column) -> columnToField.put(column, accessor.toMutator()));
		this.rowTransformer = new EmbeddedBeanRowTransformer(beanFactory, (Map) columnToField);
		this.columns = (Set<Column<T, Object>>) (Set) new KeepOrderSet<>(rowTransformer.getColumnToMember().keySet());
		
		// computing insertable columns
		this.insertableProperties = new KeepOrderMap<>();
		this.propertyToColumn.forEach((accessor, column) -> {
			// auto-incremented columns mustn't be inserted
			if (!column.isAutoGenerated()) {
				this.insertableProperties.put(accessor, column);
			}
		});
		
		// computing updatable columns
		this.updatableProperties = new KeepOrderMap<>();
		Set<Column<T, Object>> columnsWithoutPrimaryKey = targetTable.getColumnsNoPrimaryKey();
		// primary key columns are not updatable
		this.propertyToColumn.forEach((accessor, column) -> {
			if (columnsWithoutPrimaryKey.contains(column)) {
				this.updatableProperties.put(accessor, column);
			}
		});
	}
	
	public Class<C> getClassToPersist() {
		return classToPersist;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	/**
	 * @return an immutable {@link Map} of the configured mapping
	 */
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getPropertyToColumn() {
		return Collections.unmodifiableMap(propertyToColumn);
	}
	
	/**
	 * @return an immutable {@link Map} of the configured readonly mapping
	 */
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getReadonlyPropertyToColumn() {
		return Collections.unmodifiableMap(readonlyPropertyToColumn);
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
		return readConverters;
	}
	
	public void setReadConverters(ValueAccessPointMap<C, ? extends Converter<Object, Object>> converters) {
		this.readConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) converters;
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>> getWriteConverters() {
		return writeConverters;
	}
	
	public void setWriteConverters(ValueAccessPointMap<C, ? extends Converter<Object, Object>> writeConverters) {
		this.writeConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) writeConverters;
	}
	
	/**
	 * Gives mapped columns (can be a subset of the target table)
	 * @return target mapped columns
	 */
	@Override
	public Set<Column<T, ?>> getColumns() {
		return Collections.unmodifiableSet(columns);
	}
	
	public Set<Column<T, ?>> getInsertableColumns() {
		return Collections.unmodifiableSet(new KeepOrderSet<>(insertableProperties.values()));
	}
	
	public Set<Column<T, ?>> getUpdatableColumns() {
		return Collections.unmodifiableSet(new KeepOrderSet<>(updatableProperties.values()));
	}
	
	@Override
	public RowTransformer<C> getRowTransformer() {
		return rowTransformer;
	}
	
	/**
	 * Changes current {@link DefaultValueDeterminer}
	 * 
	 * @param defaultValueDeterminer a {@link DefaultValueDeterminer}
	 */
	public void setDefaultValueDeterminer(DefaultValueDeterminer defaultValueDeterminer) {
		this.defaultValueDeterminer = defaultValueDeterminer;
	}
	
	@Override
	public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
		shadowColumnsForInsert.add(valueProvider);
	}
	
	@Override
	public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		shadowColumnsForUpdate.add(valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		columns.add((Column<T, Object>) column);
	}
	
	Collection<ShadowColumnValueProvider<C, T>> getShadowColumnsForInsert() {
		return Collections.unmodifiableCollection(shadowColumnsForInsert);
	}
	
	Collection<ShadowColumnValueProvider<C, T>> getShadowColumnsForUpdate() {
		return Collections.unmodifiableCollection(shadowColumnsForUpdate);
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint<C> accessor) {
		this.propertiesSetByConstructor.add(accessor);
	}
	
	@Override
	public Map<Column<T, ?>, Object> getInsertValues(C c) {
		Map<Column<T, ?>, Object> result = new KeepOrderMap<>();
		insertableProperties.forEach((accessor, column) -> {
			Object value = accessor.get(c);
			// applying data converter if specified
			Converter<Object, Object> converter = writeConverters.get(accessor);
			if (converter != null) {
				value = converter.convert(value);
			}
			result.put(column, value);
		});
		shadowColumnsForInsert.forEach(shadowColumnValueProvider -> {
			if (shadowColumnValueProvider.accept(c)) {
				result.putAll(shadowColumnValueProvider.giveValue(c));
			}
		});
		return result;
	}
	
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, ?>, Object> unmodifiedColumns = new KeepOrderMap<>();
		// getting differences
		Map<UpwhereColumn<T>, Object> modifiedFields = new KeepOrderMap<>();
		this.updatableProperties.forEach((accessor, column) -> {
			Object modifiedValue = accessor.get(modified);
			// applying data converter if specified
			Converter<Object, Object> converter = writeConverters.get(accessor);
			if (converter != null) {
				modifiedValue = converter.convert(modifiedValue);
			}
			
			Object unmodifiedValue = unmodified == null ? null : accessor.get(unmodified);
			if (!Predicates.equalOrNull(modifiedValue, unmodifiedValue)
					// OR is here to take cases where getUpdateValues(..) gets only "modified" parameter (such as for updateById) and
					// some modified properties are null, without this OR such properties won't be updated (set to null)
					// and, overall, if they are all null, modifiedFields is empty then causing a statement without values 
					|| unmodified == null) {
				modifiedFields.put(new UpwhereColumn<>(column, true), modifiedValue);
			} else {
				unmodifiedColumns.put(column, modifiedValue);
			}
		});
		
		shadowColumnsForUpdate.forEach(shadowColumnValueProvider -> {
			if (shadowColumnValueProvider.accept(modified)) {
				if (modified != null && unmodified == null) {
					Map<Column<T, ?>, ?> modifiedValues = shadowColumnValueProvider.giveValue(modified);
					modifiedValues.forEach((col, value) -> modifiedFields.put(new UpwhereColumn<>(col, true), value));
				} else if (modified == null && unmodified != null) {
					Set<Column<T, ?>> shadowColumns = shadowColumnValueProvider.getColumns();
					shadowColumns.forEach(col -> modifiedFields.put(new UpwhereColumn<>(col, true), null));
				} else if (modified != null && unmodified != null) {
					Map<Column<T, ?>, ?> modifiedValues = shadowColumnValueProvider.giveValue(modified);
					Map<Column<T, ?>, ?> unmodifiedValues = shadowColumnValueProvider.giveValue(unmodified);
					
					Set<Column<T, ?>> shadowColumns = shadowColumnValueProvider.getColumns();
					shadowColumns.forEach(col -> {
						Object modifiedValue = modifiedValues.get(col);
						if (!Predicates.equalOrNull(modifiedValue, unmodifiedValues.get(col))) {
							modifiedFields.put(new UpwhereColumn<>(col, true), modifiedValue);
						} else {
							unmodifiedColumns.put(col, modifiedValue);
						}
					});
				}
			}
		});
		
		// adding complementary columns if necessary
		if (!modifiedFields.isEmpty() && allColumns) {
			for (Entry<Column<T, ?>, Object> unmodifiedField : unmodifiedColumns.entrySet()) {
				modifiedFields.put(new UpwhereColumn<>(unmodifiedField.getKey(), true), unmodifiedField.getValue());
			}
		}
		return modifiedFields;
	}
	
	@Override
	public C transform(ColumnedRow row) {
		// NB: please note that this transformer will determine intermediary bean instantiation through isDefaultValue(..)
		return this.rowTransformer.transform(row);
	}
	
	/**
	 * Transformer aimed at detecting if bean instance must be created according to row values (used in the select phase).
	 * For example, if all column values are null, bean will not be created.
	 */
	private class EmbeddedBeanRowTransformer extends ToBeanRowTransformer<C> {
		
		public EmbeddedBeanRowTransformer(Function<? extends ColumnedRow, C> beanFactory,
										  Map<? extends Column<?, ?>, ? extends Mutator<C, Object>> columnToMember) {
			super(beanFactory, columnToMember);
		}
		
		@Override
		public void applyRowToBean(ColumnedRow values, C targetRowBean) {
			// Algorithm is a bit complex due to embedded beans into this embedded one and the fact that we may not instantiate them
			// if all of their attributes has default values in current row : because instantiation is done silently by AccessorChainMutator
			// (the object that let us set embedded bean attributes) we have to check the need of their invocation before ... their invocation.
			// Therefore, we keep a boolean indicating if their values are default ones (if true its means that bean shouldn't be instantiated)
			// per accessor to this bean. Example : with the mappings "Person::getTimestamp, Timestamp::setCreationDate" and
			// "Person::getTimestamp, Timestamp::setModificationDate", then we keep a boolean per "Person::getTimestamp" saying that if one of
			// creationDate and modificationDate is not null then we should instantiate Timestamp, if both are null then not. 
			Map<Entry<Column<?, ?>, Mutator<C, Object>>, Object> beanValues = new HashMap<>();
			Map<Accessor, MutableBoolean> valuesAreDefaultOnes = new HashMap<>();
			for (Entry<Column<?, ?>, Mutator<C, Object>> columnFieldEntry : getColumnToMember().entrySet()) {
				Object propertyValue = values.get(columnFieldEntry.getKey());
				// applying data converter if specified
				Converter<Object, Object> converter = readConverters.get(columnFieldEntry.getValue());
				if (converter != null) {
					propertyValue = converter.convert(propertyValue);
				}
				beanValues.put(columnFieldEntry, propertyValue);
				if (columnFieldEntry.getValue() instanceof AccessorChainMutator) {
					Accessor valuesAreDefaultOnesKey = (Accessor) ((AccessorChainMutator) columnFieldEntry.getValue()).getAccessors().get(0);
					MutableBoolean mutableBoolean = valuesAreDefaultOnes.computeIfAbsent(valuesAreDefaultOnesKey, k -> new MutableBoolean(true));
					boolean valueIsDefault = EmbeddedClassMapping.this.defaultValueDeterminer.isDefaultValue(
							new Duo<>(columnFieldEntry.getKey(), columnFieldEntry.getValue()), propertyValue);
					mutableBoolean.and(valueIsDefault);
				}
			}
			// we apply values only if one of them is not a default one, because if all values are default one there's no reason that we create the bean
			beanValues.forEach((mapping, value) -> {
				if (mapping.getValue() instanceof AccessorChainMutator) {
					Accessor valuesAreDefaultOnesKey = (Accessor) ((AccessorChainMutator) mapping.getValue()).getAccessors().get(0);
					boolean valueIsDefault = valuesAreDefaultOnes.get(valuesAreDefaultOnesKey).value();
					if (!valueIsDefault) {
						applyValueToBean(targetRowBean, mapping, value);
					}
				} else {
					applyValueToBean(targetRowBean, mapping, value);
				}
			});
		}
		
		@Override
		protected void applyValueToBean(C targetRowBean, Entry<? extends Column, ? extends Mutator<C, Object>> columnFieldEntry, Object propertyValue) {
			// we skip properties set by constructor
			if (!propertiesSetByConstructor.contains(columnFieldEntry.getValue())) {
				super.applyValueToBean(targetRowBean, columnFieldEntry, propertyValue);
			}
		}
		
		private class MutableBoolean {
			
			private boolean value;
			
			private MutableBoolean(boolean value) {
				this.value = value;
			}
			
			public boolean value() {
				return value;
			}
			
			public void and(boolean otherValue) {
				this.value &= otherValue;
			}
		}
	}
	
	/**
	 * Small contract that helps to determine if a value is a default one for a bean property.
	 * Aimed at deciding if embedded beans must be instanced or not according to row values (from {@link java.sql.ResultSet}
	 * during the conversion phase
	 */
	public interface DefaultValueDeterminer {

		/**
		 * Default implementation considers null as a default value for non-primitive types, and default primitive type values as such for
		 * primitive types (took in {@link Reflections#PRIMITIVE_DEFAULT_VALUES}).
		 * 
		 * @param mappedProperty column and its mapped property (configured in the {@link EmbeddedClassMapping}).
		 * 						 So one can use either the column or the accessor for fine-grained default value determination
		 * @param value value coming from a JDBC {@link java.sql.ResultSet}, mapped by the couple {@link Column} + {@link Mutator}.
		 * @return true if value is a default one for column/property
		 */
		default boolean isDefaultValue(Duo<Column, Mutator> mappedProperty, Object value) {
			// we consider accessor type as more fine grained than Column one, hence we check default value with it
			Class inputType = Accessors.giveInputType(mappedProperty.getRight());
			return (!inputType.isPrimitive() && value == null)
					|| (inputType.isPrimitive() && Reflections.PRIMITIVE_DEFAULT_VALUES.get(inputType) == value);
		}
		
	}
}
