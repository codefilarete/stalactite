package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.bean.InstanceMethodIterator;
import org.gama.lang.bean.MethodIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorChainMutator;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.IReversibleMutator;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferences;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointByMethod;
import org.gama.reflection.ValueAccessPointComparator;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractInset;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.ImportedInset;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingConfiguration.IFluentEmbeddableMappingConfigurationEmbedOptions;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.bean.Objects.not;
import static org.gama.lang.bean.Objects.preventNull;
import static org.gama.lang.collection.Iterables.collectToList;
import static org.gama.lang.collection.Iterables.intersect;
import static org.gama.lang.collection.Iterables.minus;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.reflection.MethodReferences.toMethodReferenceString;

/**
 * Engine that converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
 *
 * @author Guillaume Mary
 * @see #build(Dialect, Table) 
 */
class EmbeddableMappingBuilder<C> {
	
	protected final EmbeddableMappingConfiguration<C> mappingConfiguration;
	protected Dialect dialect;
	protected Table targetTable;
	/** Result of {@link #build(Dialect, Table)}, shared between methods */
	protected Map<IReversibleAccessor, Column> result;
	
	EmbeddableMappingBuilder(EmbeddableMappingConfiguration<C> mappingConfiguration) {
		this.mappingConfiguration = mappingConfiguration;
	}
	
	/**
	 * Converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a {@link Map} representing the definition of the mapping declared by the {@link EmbeddableMappingConfiguration}
	 */
	Map<IReversibleAccessor, Column> build(Dialect dialect, Table targetTable) {
		this.result = new HashMap<>();
		this.dialect = dialect;
		this.targetTable = targetTable;
		// first we add mapping coming from inheritance, then it can be overwritten by class mapping 
		includeInheritance();
		// converting direct mapping
		includeDirectMapping();
		// adding embeddable (no particular thinking about order compared to previous inherited & class mapping) 
		includeEmbeddedMapping();
		return result;
	}
	
	protected Table getTargetTable() {
		return targetTable;
	}
	
	protected void includeInheritance() {
		result.putAll(buildMappingFromInheritance());
	}
	
	protected void includeDirectMapping() {
		mappingConfiguration.getPropertiesMapping().forEach(this::includeMapping);
	}
	
	protected void includeMapping(Linkage linkage) {
		Column column = addLinkage(linkage);
		ensureColumnBinding(linkage, column);
		result.put(linkage.getAccessor(), column);
	}
	
	protected void includeEmbeddedMapping() {
		result.putAll(buildEmbeddedMapping());
	}
	
	protected Column addLinkage(Linkage linkage) {
		Column addedColumn = targetTable.addColumn(linkage.getColumnName(), linkage.getColumnType());
		addedColumn.setNullable(linkage.isNullable());
		return addedColumn;
	}
	
	protected void ensureColumnBinding(Linkage linkage, Column column) {
		// assert that column binder is registered : it will throw en exception if the binder is not found
		if (linkage.getParameterBinder() != null) {
			dialect.getColumnBinderRegistry().register(column, linkage.getParameterBinder());
		} else {
			try {
				dialect.getColumnBinderRegistry().getBinder(column);
			} catch (BindingException e) {
				throw new MappingConfigurationException(column.getAbsoluteName() + " has no matching binder,"
						+ " please consider adding one to dialect binder registry or use one of the "
						+ toMethodReferenceString(
						(SerializableBiFunction<IFluentEmbeddableMappingConfiguration, SerializableFunction, IFluentEmbeddableMappingConfigurationEmbedOptions>)
								IFluentEmbeddableMappingConfiguration::embed) + " methods"
				);
			}
		}
	}
	
	protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
		Map<IReversibleAccessor, Column> inheritanceResult = new HashMap<>();
		if (mappingConfiguration.getMappedSuperClassConfiguration() != null) {
			EmbeddableMappingBuilder<? super C> superMappingBuilder = new EmbeddableMappingBuilder<>(mappingConfiguration.getMappedSuperClassConfiguration());
			Map<IReversibleAccessor, Column> columnMap = superMappingBuilder.build(dialect, targetTable);
			inheritanceResult.putAll(columnMap);
		}
		return inheritanceResult;
	}
	
	/**
	 * Collects mapping of given {@link EmbeddedBeanMappingStrategy} and created equivalent {@link Column} on the given target table.
	 *
	 * @param mappingStrategy the source of mapping
	 * @param localTargetTable the table on which columns must be added
	 * @return a mapping between properties of {@link EmbeddedBeanMappingStrategy#getPropertyToColumn()} and their column in our
	 * {@link #targetTable}
	 */
	protected Map<IReversibleAccessor, Column> collectMapping(IMappingStrategy<? super C, ?> mappingStrategy,
															  Table localTargetTable, BiFunction<IReversibleAccessor, Column, String> columnNameSupplier) {
		Map<IReversibleAccessor, Column> localResult = new HashMap<>();
		Map<? extends IReversibleAccessor<? super C, Object>, ? extends Column<?, Object>> propertyToColumn = mappingStrategy.getPropertyToColumn();
		propertyToColumn.forEach((accessor, column) -> {
			Column projectedColumn = localTargetTable.addColumn(columnNameSupplier.apply(accessor, column), column.getJavaType());
			projectedColumn.setAutoGenerated(column.isAutoGenerated());
			projectedColumn.setNullable(column.isNullable());
			localResult.put(accessor, projectedColumn);
		});
		return localResult;
	}
	
	private Map<IReversibleAccessor, Column> buildEmbeddedMapping() {
		Map<String, Column<Table, Object>> columnsPerName = targetTable.mapColumnsOnName();
		Map<IReversibleAccessor, Column> toReturn = new HashMap<>();
		Set<AbstractInset<C, ?>> treatedInsets = new HashSet<>();
		// Registry of inner embedded properties, because they must be excluded during build process
		Set<ValueAccessPoint> innerEmbeddedBeanRegistry = new ValueAccessPointSet();
		mappingConfiguration.getInsets().stream().filter(Inset.class::isInstance).map(Inset.class::cast)
				.forEach(i -> innerEmbeddedBeanRegistry.add(i.getAccessor()));
		
		// starting mapping
		mappingConfiguration.getInsets().forEach(inset -> {
			assertNotAlreadyMapped(inset, treatedInsets);
			
			ValueAccessPointMap<String> overridenColumnNames = inset.getOverridenColumnNames();
			
			if (inset instanceof FluentEmbeddableMappingConfigurationSupport.ImportedInset) {
				Table pawnTable = new Table("pawnTable");
				EmbeddedBeanMappingStrategy embeddedBeanMappingStrategy = ((ImportedInset<C, ?>) inset).getBeanMappingBuilder().build(dialect, pawnTable);
				Map<IReversibleAccessor, Column> embeddedBeanMapping = embeddedBeanMappingStrategy.getPropertyToColumn();
				// looking for conflicting columns between those expected by embeddable bean and those of the entity
				Map<ValueAccessPoint, Column> intermediaryResult = new ValueAccessPointMap<>();
				intermediaryResult.putAll(result);
				intermediaryResult.putAll(toReturn);
				Map<String, ValueAccessPoint> alreadyMappedPropertyPerColumnName = Iterables.map(intermediaryResult.entrySet(),
						e -> e.getValue().getName(), Entry::getKey);
				Map<ValueAccessPoint, Column> notOverridenEmbeddedProperties = new ValueAccessPointMap<>(embeddedBeanMapping);
				notOverridenEmbeddedProperties.keySet().removeIf(overridenColumnNames::containsKey);
				Map<String, ValueAccessPoint> notOverridenColumnNames = Iterables.map(notOverridenEmbeddedProperties.entrySet(),
						e -> e.getValue().getName(), Entry::getKey);
				
				// conflicting mapping are properties on both side : neither overriden
				Set<String> conflictingMapping = intersect(notOverridenColumnNames.keySet(), alreadyMappedPropertyPerColumnName.keySet());
				
				if (!conflictingMapping.isEmpty()) {
					// looking for conflicting properties for better exception message
					class Conflict {
						private final ValueAccessPoint embeddableBeanProperty;
						private final ValueAccessPoint entityProperty;
						private final String columnName;
						
						private Conflict(ValueAccessPoint embeddableBeanProperty, ValueAccessPoint entityProperty, String columnName) {
							this.embeddableBeanProperty = embeddableBeanProperty;
							this.entityProperty = entityProperty;
							this.columnName = columnName;
						}
						
						@Override
						public String toString() {
							return "Embeddable definition '" + MemberDefinition.toString(embeddableBeanProperty)
									+ "' vs entity definition '" + MemberDefinition.toString(entityProperty)
									+ "' on column name '" + columnName + "'";
						}
					}
					List<Conflict> conflicts = new ArrayList<>();
					for (String conflict : conflictingMapping) {
						conflicts.add(new Conflict(notOverridenColumnNames.get(conflict), alreadyMappedPropertyPerColumnName.get(conflict), conflict));
					}
					throw new MappingConfigurationException(
							"Some embedded columns conflict with entity ones on their name, please override it or change it :"
							+ System.lineSeparator()
							+ new StringAppender().ccat(conflicts, System.lineSeparator()));
				}
				
				collectMapping(embeddedBeanMappingStrategy, targetTable, (a, c) -> overridenColumnNames.getOrDefault(a, c.getName()));
				
				Set<ValueAccessPoint> embeddedBeanMappingRegistry = new ValueAccessPointSet(embeddedBeanMapping.keySet());
				overridenColumnNames.forEach((valueAccessPoint, nameOverride) -> {
					if (embeddedBeanMappingRegistry.contains(valueAccessPoint)) {
						MemberDefinition memberDefinition = MemberDefinition.giveMemberDefinition(valueAccessPoint);
						Column addedColumn = targetTable.addColumn(nameOverride, memberDefinition.getMemberType());
						// adding the newly created column to our index for next iterations because it may conflicts with mapping of other iterations
						columnsPerName.put(nameOverride, addedColumn);
					} else {
						// Case : property is not mapped by embeddable strategy, so overriding it has no purpose
						String methodSignature = MemberDefinition.toString(valueAccessPoint);
						throw new MappingConfigurationException(methodSignature + " is not mapped by embeddable strategy,"
								+ " so its column name override '" + nameOverride + "' can't apply");
					}
				});
				toReturn.putAll(embeddedBeanMapping);
			} else if (inset instanceof Inset) {
				Inset refinedInset = (Inset) inset;
				
				List<ValueAccessPoint> rootAccessors = collectToList(() -> new HierarchyIterator(refinedInset), Inset::getAccessor);
				Collections.reverse(rootAccessors);
				
				// we add properties of the embedded bean
				Stream<ValueAccessPointByMethod> getterAndSetterStream = giveMappableMethodStream(refinedInset, innerEmbeddedBeanRegistry);
				getterAndSetterStream.forEach(valueAccessPoint -> {
					MemberDefinition memberDefinition = MemberDefinition.giveMemberDefinition(valueAccessPoint);
					// looking for the targeted column
					Column targetColumn = findColumn(valueAccessPoint, memberDefinition.getName(), columnsPerName, refinedInset);
					if (targetColumn == null) {
						// Column isn't declared in table => we create one from field informations
						String columnName = mappingConfiguration.getColumnNamingStrategy().giveName(new MemberDefinition(
								valueAccessPoint.getMethod().getDeclaringClass(),
								valueAccessPoint.getMethod().getName(),
								Reflections.onJavaBeanPropertyWrapperName(valueAccessPoint.getMethod(), Method::getReturnType, method -> method.getParameterTypes()[0], Method::getReturnType)
						));
						String overridenName = overridenColumnNames.get(valueAccessPoint);
						if (overridenName != null) {
							columnName = overridenName;
						}
						targetColumn = targetTable.addColumn(columnName, memberDefinition.getMemberType());
						// adding the newly created column to our index for next iterations because it may conflicts with mapping of other iterations
						columnsPerName.put(columnName, targetColumn);
					} else {
						// checking that column is not already mapped by a previous definition
						Column finalTargetColumn = targetColumn;
						Optional<Entry<IReversibleAccessor, Column>> existingMapping =
								EmbeddableMappingBuilder.this.result.entrySet().stream().filter(entry -> entry.getValue().equals(finalTargetColumn)).findFirst();
						if (existingMapping.isPresent()) {
							Method currentMethod = inset.getInsetAccessor();
							String currentMethodReference = toMethodReferenceString(currentMethod);
							throw new MappingConfigurationException("Error while mapping "
									+ currentMethodReference + " : " + memberDefinition.toString()
									+ " conflicts with " + MemberDefinition.toString(existingMapping.get().getKey()) + " because they use same column" +
									", override one of their name to avoid the conflict" +
									", see " + MethodReferences.toMethodReferenceString(
									(SerializableTriFunction<EmbedOptions, SerializableFunction, String, EmbedOptions>) EmbedOptions::overrideName));
						}
					}
					
					String chainDescription = inset.getAccessor() + " > " + memberDefinition.toString();
					AccessorChain chain = getAccessorChain(rootAccessors, valueAccessPoint, chainDescription);
					toReturn.put(chain, targetColumn);
				});
			}
			treatedInsets.add(inset);
		});
		return toReturn;
	}
	
	private AccessorChain getAccessorChain(List<ValueAccessPoint> rootAccessors, ValueAccessPointByMethod terminalAccessor, String chainDescription) {
		// we must clone rootAccessors to prevent from accessor mixing
		rootAccessors = new ArrayList<>(rootAccessors);
		IAccessor accessor;
		if (terminalAccessor instanceof IAccessor) {
			accessor = (IAccessor) terminalAccessor;
		} else if (terminalAccessor instanceof IMutator) {
			accessor = ((IReversibleMutator) terminalAccessor).toAccessor();
		} else {
			// Something has change in ValueAccessPoint hierarchy, it should be fixed (grave)
			throw new IllegalArgumentException(Reflections.toString(terminalAccessor.getClass()) + " is unknown from chain creation algorithm");
		}
		rootAccessors.add(accessor);
		// we create a chain that
		// - returns null when beans are not instanciated, so null will be inserted/updated in embedded columns
		// - initializes values when its mutator will be used, so bean will create its embedded properties on select
		// (voluntary dissimetric behavior)
		return new AccessorChain(rootAccessors) {
			@Override
			public AccessorChainMutator toMutator() {
				AccessorChainMutator toReturn = super.toMutator();
				toReturn.setNullValueHandler(AccessorChain.INITIALIZE_VALUE);
				return toReturn;
			}
			
			@Override
			public String toString() {
				return chainDescription;
			}
		}.setNullValueHandler(AccessorChain.RETURN_NULL);
	}
	
	private Stream<ValueAccessPointByMethod> giveMappableMethodStream(Inset<C, ?> inset, Set<ValueAccessPoint> innerEmbeddableRegistry) {
		InstanceMethodIterator methodIterator = new InstanceMethodIterator(inset.getEmbeddedClass(), Object.class);
		ValueAccessPointSet excludedProperties = inset.getExcludedProperties();
		// NB: we skip fields that are registered as inset (inner embeddable) because they'll be treated by their own, skipping them avoids conflicts
		Predicate<ValueAccessPoint> innerEmbeddableExcluder = not(innerEmbeddableRegistry::contains);
		// we also skip excluded fields by user
		Predicate<ValueAccessPoint> excludedFieldsExcluder = not(excludedProperties::contains);
		
		return stream(() -> methodIterator).map(m ->
				(ValueAccessPointByMethod) Reflections.onJavaBeanPropertyWrapperNameGeneric(m.getName(), m,
						AccessorByMethod::new,
						MutatorByMethod::new,
						AccessorByMethod::new,
						method -> null /* non Java Bean naming convention compliant method */))
				.filter(java.util.Objects::nonNull)
				.filter(innerEmbeddableExcluder.and(excludedFieldsExcluder));
	}
	
	private void assertNotAlreadyMapped(AbstractInset<C, ?> inset, Set<AbstractInset<C, ?>> treatedInsets) {
		Optional<AbstractInset<C, ?>> alreadyMappedType = treatedInsets.stream().filter(i -> i.getEmbeddedClass() == inset.getEmbeddedClass()).findFirst();
		if (alreadyMappedType.isPresent()) {
			// accessors are exactly the same ?
			if (alreadyMappedType.get().getInsetAccessor().equals(inset.getInsetAccessor())) {
				Method currentMethod = inset.getInsetAccessor();
				String currentMethodReference = toMethodReferenceString(currentMethod);
				throw new MappingConfigurationException(currentMethodReference + " is already mapped");
			}
			if (inset instanceof Inset<?, ?>) {
				// else : type is already mapped throught a different property
				// we're going to check if all subproperties override their column name, else an exception will be thrown
				// to prevent 2 properties from being mapped on same column
				MethodIterator methodIterator = new MethodIterator(inset.getEmbeddedClass());
				Set<ValueAccessPoint> expectedOverridenFields = new ValueAccessPointSet();
				// NB: stream is sorted to get a consistent result over executions because MethodIterator doesn't always return methods in same
				// order, probably because JVM doesn't provide methods in a steady order over executions too. Mainly done for unit test checking.
				stream(() -> methodIterator).sorted(Comparator.comparing(Reflections::toString)).forEach(m ->
					Nullable.nullable((ValueAccessPoint) Reflections.onJavaBeanPropertyWrapperNameGeneric(m.getName(), m,
							AccessorByMethod::new,
							MutatorByMethod::new,
							AccessorByMethod::new,
							method -> null)).accept(expectedOverridenFields::add)
				);
				expectedOverridenFields.removeAll(inset.getExcludedProperties());
				Set<ValueAccessPoint> overridenFields = inset.getOverridenColumnNames().keySet();
				boolean allFieldsAreOverriden = overridenFields.equals(expectedOverridenFields);
				if (!allFieldsAreOverriden) {
					Method currentMethod = inset.getInsetAccessor();
					String currentMethodReference = toMethodReferenceString(currentMethod);
					Method conflictingMethod = alreadyMappedType.get().getInsetAccessor();
					String conflictingDeclaration = toMethodReferenceString(conflictingMethod);
					expectedOverridenFields.removeAll(overridenFields);
					throw new MappingConfigurationException(
							currentMethodReference + " conflicts with " + conflictingDeclaration + " while embedding a " + Reflections.toString(inset.getEmbeddedClass())
									+ ", column names should be overriden : "
									+ minus(expectedOverridenFields, overridenFields, new ValueAccessPointComparator())
									.stream().map(MemberDefinition::toString).collect(Collectors.joining(", ")));
				}
			}
		}
	}
	
	/**
	 * Expected to give the {@link Column} for the given {@link Field} and {@link Inset} in the target {@link Table} (represented by its column
	 * registry {@code tableColumnsPerName}).
	 *
	 * @param valueAccessPoint the property to be mapped
	 * @param defaultColumnName name to search if not overriden
	 * @param tableColumnsPerName persistence table's column registry
	 * @param configuration mapping configuration
	 * @return the {@link Column} of the target table that should be used to the field
	 */
	protected Column findColumn(ValueAccessPoint valueAccessPoint, String defaultColumnName, Map<String, Column<Table, Object>> tableColumnsPerName, Inset<C, ?> configuration) {
		String columnNameToSearch = preventNull(configuration.getOverridenColumnNames().get(valueAccessPoint), defaultColumnName);
		return tableColumnsPerName.get(columnNameToSearch);
	}
	
	private static class HierarchyIterator implements Iterator<Inset> {
		
		private Inset current;
		
		private HierarchyIterator(Inset starter) {
			this.current = starter;
		}
		
		@Override
		public boolean hasNext() {
			return current != null;
		}
		
		@Override
		public Inset next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			Inset result = this.current;
			current = current.getParent();
			return result;
		}
	}
}