package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.reflection.AccessorChainMutator;
import org.gama.reflection.Accessors;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Persistence strategy for simple "embedded" bean (no identifier nor relation managed here) : kind of straight mapping betwen some properties
 * of a class and some columns of a table.
 * 
 * @author Guillaume Mary
 */
public class EmbeddedBeanMappingStrategy<C, T extends Table> implements IEmbeddedBeanMappingStrategy<C, T> {
	
	private final Class<C> classToPersist;
	
	private final T targetTable;
	
	private final Map<IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn;
	
	private final Set<Column<T, Object>> columns;
	
	private final Set<Column<T, Object>> insertableColumns;
	
	/** Acts as a cache of updatable properties, could be dynamically deduced from {@link #propertyToColumn} and {@link #insertableColumns} */
	private final Map<IReversibleAccessor<C, Object>, Column<T, Object>> insertableProperties;
	
	private final Set<Column<T, Object>> updatableColumns;
	
	/** Acts as a cache of updatable properties, could be dynamically deduced from {@link #propertyToColumn} and {@link #updatableColumns} */
	private final Map<IReversibleAccessor<C, Object>, Column<T, Object>> updatableProperties;
	
	private final ToBeanRowTransformer<C> rowTransformer;
	
	private DefaultValueDeterminer defaultValueDeterminer = new DefaultValueDeterminer() {};
	
	/**
	 * Columns (and their value provider) which are not officially mapped by a bean property.
	 * Those are for insertion time.
	 */
	private final List<ShadowColumnValueProvider<C, Object, T>> shadowColumnsForInsert = new ArrayList<>();
	
	/**
	 * Columns (and their value provider) which are not officially mapped by a bean property.
	 * Those are for update time.
	 */
	private final List<ShadowColumnValueProvider<C, Object, T>> shadowColumnsForUpdate = new ArrayList<>();
	
	private final ValueAccessPointSet propertiesSetByConstructor = new ValueAccessPointSet();
	
	/**
	 * Build a EmbeddedBeanMappingStrategy from a mapping between Field and Column.
	 * Fields are expected to be from same class.
	 * Columns are expected to be from same table.
	 * No control is done about that, caller must be aware of it.
	 * First entry of {@code propertyToColumn} is used to pick up persisted class and target table.
	 *
	 * @param classToPersist the class to be persisted
	 * @param targetTable the persisting table
	 * @param propertyToColumn a mapping between Field and Column, expected to be coherent (fields of same class, column of same table)
	 */
	public EmbeddedBeanMappingStrategy(Class<C> classToPersist, T targetTable, Map<? extends IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn) {
		this(classToPersist, targetTable, propertyToColumn, row -> Reflections.newInstance(classToPersist));
	}
	
	public EmbeddedBeanMappingStrategy(Class<C> classToPersist,
									   T targetTable,
									   Map<? extends IReversibleAccessor<C, Object>, Column<T, Object>> propertyToColumn,
									   Function<Function<Column, Object>, C> beanFactory) {
		this.classToPersist = classToPersist;
		this.targetTable = targetTable;
		this.propertyToColumn = new HashMap<>(propertyToColumn);
		Map<Column<T, Object>, IMutator> columnToField = Iterables.map(propertyToColumn.entrySet(), Entry::getValue, e -> e.getKey().toMutator());
		this.rowTransformer = new EmbeddedBeanRowTransformer(beanFactory, (Map) columnToField);
		this.columns = new LinkedHashSet<>(propertyToColumn.values());
		
		// computing insertable columns
		this.insertableColumns = new HashSet<>();
		this.insertableProperties = new HashMap<>();
		this.propertyToColumn.forEach((accessor, column) -> {
			// autoincremented columns mustn't be inserted
			if (!column.isAutoGenerated()) {
				this.insertableColumns.add(column);
				this.insertableProperties.put(accessor, column);
			}
		});
		
		// computing updatable columns
		this.updatableColumns = new HashSet<>();
		this.updatableProperties = new HashMap<>();
		Set<Column> columnsWithoutPrimaryKey = targetTable.getColumnsNoPrimaryKey();
		// primarykey columns are not updatable
		this.propertyToColumn.forEach((accessor, column) -> {
			// primary key columns mustn't be updated
			if (columnsWithoutPrimaryKey.contains(column)) {
				this.updatableColumns.add(column);
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
	public Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		return Collections.unmodifiableMap(propertyToColumn);
	}
	
	/**
	 * Gives mapped columns (can be a subset of the target table)
	 * @return target mapped columns
	 */
	@Nonnull
	@Override
	public Set<Column<T, Object>> getColumns() {
		return Collections.unmodifiableSet(columns);
	}
	
	public Set<Column<T, Object>> getInsertableColumns() {
		return Collections.unmodifiableSet(insertableColumns);
	}
	
	public Set<Column<T, Object>> getUpdatableColumns() {
		return Collections.unmodifiableSet(updatableColumns);
	}
	
	public ToBeanRowTransformer<C> getRowTransformer() {
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
	public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> valueProvider) {
		shadowColumnsForInsert.add((ShadowColumnValueProvider<C, Object, T>) valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> valueProvider) {
		shadowColumnsForUpdate.add((ShadowColumnValueProvider<C, Object, T>) valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		columns.add((Column<T, Object>) column);
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint accessor) {
		this.propertiesSetByConstructor.add(accessor);
	}
	
	@Nonnull
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		Map<Column<T, Object>, Object> result = new HashMap<>();
		insertableProperties.forEach((prop, value) -> result.put(value, prop.get(c)));
		shadowColumnsForInsert.forEach(shadowColumnValueProvider -> {
			if (shadowColumnValueProvider.accept(c)) {
				result.put(shadowColumnValueProvider.getColumn(), shadowColumnValueProvider.giveValue(c));
			}
		});
		return result;
	}
	
	@Nonnull
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		Map<Column<T, Object>, Object> unmodifiedColumns = new HashMap<>();
		// getting differences
		Map<UpwhereColumn<T>, Object> modifiedFields = new HashMap<>();
		this.updatableProperties.forEach((accessor, accessorColumn) -> {
			Object modifiedValue = accessor.get(modified);
			Object unmodifiedValue = unmodified == null ? null : accessor.get(unmodified);
			if (!Predicates.equalOrNull(modifiedValue, unmodifiedValue)
					// OR is here to take cases where getUpdateValues(..) gets only "modified" parameter (such as for updateById) and
					// some modified properties are null, without this OR such properties won't be updated (set to null)
					// and, overall, if they are all null, modifiedFields is empty then causing a statement without values 
					|| unmodified == null) {
				modifiedFields.put(new UpwhereColumn<>(accessorColumn, true), modifiedValue);
			} else {
				unmodifiedColumns.put(accessorColumn, modifiedValue);
			}
		});
		
		// adding complementary columns if necessary
		if (!modifiedFields.isEmpty() && allColumns) {
			for (Entry<Column<T, Object>, Object> unmodifiedField : unmodifiedColumns.entrySet()) {
				modifiedFields.put(new UpwhereColumn<>(unmodifiedField.getKey(), true), unmodifiedField.getValue());
			}
		}
		// getting values for silent columns
		shadowColumnsForUpdate.forEach(shadowColumnValueProvider -> {
			if (shadowColumnValueProvider.accept(modified)) {
				modifiedFields.put(new UpwhereColumn<>(shadowColumnValueProvider.getColumn(), true), shadowColumnValueProvider.giveValue(modified));
			}
		});
		return modifiedFields;
	}
	
	@Override
	public C transform(Row row) {
		// NB: please note that this transfomer will determine intermediary bean instanciation through isDefaultValue(..)
		return this.rowTransformer.transform(row);
	}
	
	@Override
	public AbstractTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return getRowTransformer().copyWithAliases(columnedRow);
	}
	
	/**
	 * Transformer aimed at detecting if bean instance must be created according to row values (used in the select phase).
	 * For example, if all column values are null, bean will not be created.
	 */
	private class EmbeddedBeanRowTransformer extends ToBeanRowTransformer<C> {
		
		public EmbeddedBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, IMutator> columnToMember) {
			super(beanFactory, columnToMember);
		}
		
		/**
		 * Constructor for private copy, see {@link #copyWithAliases(ColumnedRow)}
		 * @param beanFactory method that creates instance
		 * @param columnToMember mapping between database columns and bean properties
		 * @param columnedRow mapping between {@link Column} and their alias in given row to {@link #transform(Row)} 
		 * @param rowTransformerListeners listeners that need notification of bean creation
		 */
		private EmbeddedBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, IMutator> columnToMember,
										   ColumnedRow columnedRow, Collection<TransformerListener<C>> rowTransformerListeners) {
			super(beanFactory, columnToMember, columnedRow, rowTransformerListeners);
		}
		
		@Override
		public void applyRowToBean(Row values, C targetRowBean) {
			// Algorithm is a little but complex due to embedded beans into this embedded one and the fact that we may not instanciate them
			// if all of their attributes has default values in current row : because instanciation is done silently by AccessorChainMutator
			// (the object that let us set embedded bean attributes) we have to check the need of their invokation before ... their invokation.
			// Therefore we keep a boolean indicating if their values are default ones (if true its means that bean shouldn't be instanciated)
			// per accessor to this bean. Example : with the mappings "Person::getTimestamp, Timestamp::setCreationDate" and
			// "Person::getTimestamp, Timestamp::setModificationDate", then we keep a boolean per "Person::getTimestamp" saying that if one of
			// creationDate and modificationDate is not null then we should instanciate Timestamp, if both are null then not. 
			Map<Entry<Column, IMutator>, Object> beanValues = new HashMap<>();
			Map<IAccessor, MutableBoolean> valuesAreDefaultOnes = new HashMap<>();
			for (Entry<Column, IMutator> columnFieldEntry : getColumnToMember().entrySet()) {
				Object propertyValue = getColumnedRow().getValue(columnFieldEntry.getKey(), values);
				beanValues.put(columnFieldEntry, propertyValue);
				boolean valueIsDefault = EmbeddedBeanMappingStrategy.this.defaultValueDeterminer.isDefaultValue(
						new Duo<>(columnFieldEntry.getKey(), columnFieldEntry.getValue()), propertyValue);
				if (columnFieldEntry.getValue() instanceof AccessorChainMutator) {
					IAccessor valuesAreDefaultOnesKey = (IAccessor) ((AccessorChainMutator) columnFieldEntry.getValue()).getAccessors().get(0);
					MutableBoolean mutableBoolean = valuesAreDefaultOnes.computeIfAbsent(valuesAreDefaultOnesKey, k -> new MutableBoolean(true));
					mutableBoolean.and(valueIsDefault);
				}
			}
			// we apply values only if one of them is not a default one, because if all values are default one there's no reason that we create the bean
			beanValues.forEach((mapping, value) -> {
				if (mapping.getValue() instanceof AccessorChainMutator) {
					IAccessor valuesAreDefaultOnesKey = (IAccessor) ((AccessorChainMutator) mapping.getValue()).getAccessors().get(0);
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
		protected void applyValueToBean(C targetRowBean, Entry<Column, IMutator> columnFieldEntry, Object propertyValue) {
			// we skip properties set as defined by constructor
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
		
		@Override
		public EmbeddedBeanRowTransformer copyWithAliases(ColumnedRow columnedRow) {
			return new EmbeddedBeanRowTransformer(beanFactory,
					getColumnToMember(),
					columnedRow,
					// listeners are given to the new instance because they may be interested in transforming rows of this one
					getRowTransformerListeners()
			);
		}
	}
	
	/**
	 * Small constract that helps to determine if a value is a default one for a bean property.
	 * Aimed at deciding if embedded beans must be instanciated or not according to row values (from {@link java.sql.ResultSet}
	 * during the conversion phase
	 */
	public interface DefaultValueDeterminer {

		/**
		 * Default implementation considers null as a default value for non primitive types, and default primitive type values as such for
		 * primitive types (took in {@link Reflections#PRIMITIVE_DEFAULT_VALUES}).
		 * 
		 * @param mappedProperty column and its mapped property (configured in the {@link EmbeddedBeanMappingStrategy}).
		 * 						 So one can use either the column or the accessor for fine grained default value determination
		 * @param value value coming from a JDBC {@link java.sql.ResultSet}, mapped by the couple {@link Column} + {@link IMutator}.
		 * @return true if value is a default one for column/property
		 */
		default boolean isDefaultValue(Duo<Column, IMutator> mappedProperty, Object value) {
			// we consider accessor type as more fine grained than Column one, hence we check default value with it
			Class inputType = Accessors.giveInputType(mappedProperty.getRight());
			return (!inputType.isPrimitive() && value == null)
					|| (inputType.isPrimitive() && Reflections.PRIMITIVE_DEFAULT_VALUES.get(inputType) == value);
		}
		
	}
}
