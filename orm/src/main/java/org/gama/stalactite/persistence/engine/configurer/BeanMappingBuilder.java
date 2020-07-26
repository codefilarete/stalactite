package org.gama.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
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
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Reflections;
import org.gama.lang.bean.InstanceMethodIterator;
import org.gama.lang.bean.MethodIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.IReversibleMutator;
import org.gama.reflection.MethodReferences;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointByMethod;
import org.gama.reflection.ValueAccessPointComparator;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.EmbedOptions;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.AbstractInset;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.ImportedInset;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageByColumnName;
import org.gama.stalactite.persistence.engine.configurer.FluentEntityMappingConfigurationSupport.OverridableColumnInset;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingConfiguration.IFluentEmbeddableMappingConfigurationEmbedOptions;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.bean.Objects.not;
import static org.gama.lang.collection.Iterables.collectToList;
import static org.gama.lang.collection.Iterables.minus;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.reflection.MethodReferences.toMethodReferenceString;

/**
 * Engine that converts mapping definition of a {@link EmbeddableMappingConfiguration} to a simple {@link Map}.
 * Designed such as its instances can be reused (no constructor, attributes are set through
 * {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}
 * (hence they are not thread-safe) so one can override this class and reuse its instance to get same behavior.
 * 
 * Whereas it consumes an {@link EmbeddableMappingConfiguration} it doesn't mean that its goal is to manage embedded beans of an entity : as its
 * name says it's aimed at collecting mapping of any beans, without the entity part (understanding identification and inheritance which is targetted
 * by {@link PersisterBuilderImpl})
 * 
 * @author Guillaume Mary
 * @see #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)    
 */
class BeanMappingBuilder {
	
	private EmbeddableMappingConfiguration<?> mappingConfiguration;
	private Table targetTable;
	private ColumnNameProvider columnNameProvider;
	private ColumnBinderRegistry columnBinderRegistry;
	/** Result of {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}, shared between methods */
	protected Map<IReversibleAccessor, Column> result;
	
	/**
	 * Converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a {@link Map} representing the definition of the mapping declared by the {@link EmbeddableMappingConfiguration}
	 */
	Map<IReversibleAccessor, Column> build(EmbeddableMappingConfiguration mappingConfiguration,
												   Table targetTable, ColumnBinderRegistry columnBinderRegistry, ColumnNameProvider columnNameProvider) {
		this.mappingConfiguration = mappingConfiguration;
		this.columnNameProvider = columnNameProvider;
		this.columnBinderRegistry = columnBinderRegistry;
		this.targetTable = targetTable;
		this.result = new HashMap<>();
		// converting direct mapping
		includeDirectMapping(this.mappingConfiguration, null, new ValueAccessPointMap<>(), new ValueAccessPointMap<>(), new ValueAccessPointSet());
		// adding embeddable (no particular thought about order compared to previous direct mapping) 
		includeEmbeddedMapping();
		return result;
	}
	
	protected Table getTargetTable() {
		return targetTable;
	}
	
	protected void includeDirectMapping(EmbeddableMappingConfiguration<?> givenConfiguration, @Nullable ValueAccessPoint accessorPrefix,
										ValueAccessPointMap<String> overridenColumnNames, ValueAccessPointMap<Column> overridenColumns,
										ValueAccessPointSet excludedProperties) {
		givenConfiguration.getPropertiesMapping().stream()
				.filter(linkage -> !excludedProperties.contains(linkage.getAccessor()))
				.forEach(linkage -> includeMapping(linkage, givenConfiguration, accessorPrefix,
						overridenColumnNames.get(linkage.getAccessor()),
						overridenColumns.get(linkage.getAccessor())));
	}
	
	protected void includeMapping(Linkage linkage, EmbeddableMappingConfiguration<?> mappingConfiguration, @Nullable ValueAccessPoint accessorPrefix,
								  String overridenColumnName, Column overridenColumn) {
		String columnName = nullable(overridenColumn).map(Column::getName).getOr(() -> determineColumnName(linkage, overridenColumnName));
		assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
		Column column = nullable(overridenColumn).getOr(() -> addColumnToTable(linkage, columnName));
		ensureColumnBindingInRegistry(linkage, column);
		IReversibleAccessor accessor;
		if (accessorPrefix != null) {
			accessor = newAccessorChain(Collections.singletonList(accessorPrefix), linkage.getAccessor());
		} else {
			accessor = linkage.getAccessor();
		}
		result.put(accessor, column);
	}
	
	protected void assertMappingIsNotAlreadyDefinedByInheritance(Linkage linkage, String columnNameToCheck, EmbeddableMappingConfiguration<?> mappingConfiguration) {
		DuplicateDefinitionChecker duplicateDefinitionChecker = new DuplicateDefinitionChecker(linkage.getAccessor(), columnNameToCheck, columnNameProvider);
		stream(mappingConfiguration.inheritanceIterable())
				.flatMap(configuration -> (Stream<Linkage>) configuration.getPropertiesMapping().stream())
				// not using equals() is voluntary since we want referencial checking here to exclude same instance, prevent equals() override to break this algorithm
				.filter(pawn -> linkage != pawn)
				.forEach(duplicateDefinitionChecker);
	}
	
	protected Column addColumnToTable(Linkage linkage, String columnName) {
		Column addedColumn = targetTable.addColumn(columnName, linkage.getColumnType());
		addedColumn.setNullable(linkage.isNullable());
		return addedColumn;
	}
	
	private String determineColumnName(Linkage linkage, @Nullable String overridenColumName) {
		return nullable(overridenColumName).elseSet(linkage.getColumnName()).getOr(() -> columnNameProvider.giveColumnName(linkage));
	}
	
	protected void ensureColumnBindingInRegistry(Linkage linkage, Column column) {
		// assert that column binder is registered : it will throw en exception if the binder is not found
		if (linkage.getParameterBinder() != null) {
			columnBinderRegistry.register(column, linkage.getParameterBinder());
		} else {
			try {
				columnBinderRegistry.getBinder(column);
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
	
	/**
	 * Created equivalent {@link Column}s of those of given {@link Map} values on the given target table. May do nothing if columns already exist.
	 *
	 * @param propertyToColumn mapping between some property accessors and {@link Column}s from default target {@link Table}
	 * 							(the one given at {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}
	 * 							time)
	 * @param localTargetTable the table on which columns must be added
	 * @return a mapping between properties of given {@link Map} keys and their column in given {@link Table}
	 */
	protected static Map<IReversibleAccessor, Column> projectColumns(Map<? extends IReversibleAccessor, ? extends Column> propertyToColumn,
															  Table localTargetTable,
															  BiFunction<IReversibleAccessor, Column, String> columnNameSupplier) {
		Map<IReversibleAccessor, Column> localResult = new HashMap<>();
		propertyToColumn.forEach((accessor, column) -> {
			Column projectedColumn = localTargetTable.addColumn(columnNameSupplier.apply(accessor, column), column.getJavaType());
			projectedColumn.setAutoGenerated(column.isAutoGenerated());
			projectedColumn.setNullable(column.isNullable());
			localResult.put(accessor, projectedColumn);
		});
		return localResult;
	}
	
	/**
	 * Adds embedded beans mapping to result
	 */
	private void includeEmbeddedMapping() {
		Set<AbstractInset<?, ?>> treatedInsets = new HashSet<>();
		
		Set<ValueAccessPoint> innerEmbeddedBeanRegistry = new ValueAccessPointSet();
		mappingConfiguration.getInsets().stream().filter(Inset.class::isInstance).map(Inset.class::cast)
				.forEach(i -> innerEmbeddedBeanRegistry.add(i.getAccessor()));
		
		Queue<AbstractInset> stack = new ArrayDeque<>(mappingConfiguration.getInsets());
		while (!stack.isEmpty()) {
			AbstractInset<?, ?> inset = stack.poll();
			
			assertNotAlreadyDeclared(inset, treatedInsets);
			
			if (inset instanceof Inset) {
				Inset refinedInset = (Inset) inset;
				
				List<ValueAccessPoint> rootAccessors = collectToList(() -> new InsetChainIterator(refinedInset), Inset::getAccessor);
				Collections.reverse(rootAccessors);
				
				// we add properties of the embedded bean
				ValueAccessPointSet excludedProperties = inset.getExcludedProperties();
				Set<ValueAccessPointByMethod> getterAndSetters = giveMappableMethods(refinedInset, innerEmbeddedBeanRegistry);
				getterAndSetters.stream()
						.filter(not(excludedProperties::contains))
						.forEach(valueAccessPoint -> {
							AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
							LinkageByColumnName<?> linkage = new LinkageByColumnName<>(new AccessorByMethod<>(valueAccessPoint.getMethod()),
									accessorDefinition.getMemberType(), null);
							Column targetColumn = null;
							if (inset instanceof OverridableColumnInset) {
								targetColumn = ((OverridableColumnInset<?, ?>) inset).getOverridenColumns().get(valueAccessPoint);
							}
							if (targetColumn == null) {
								String columnName = determineColumnName(linkage, inset.getOverridenColumnNames().get(valueAccessPoint));
								targetColumn = addColumnToTable(linkage, columnName);
								ensureColumnBindingInRegistry(linkage, targetColumn);
							}
							// checking that column is not already mapped by a previous definition
							Column finalTargetColumn = targetColumn;
							Entry<IReversibleAccessor, Column> existingMapping = Iterables.find(result.entrySet(),
									entry -> entry.getValue().equals(finalTargetColumn));
							if (existingMapping != null) {
								Method currentMethod = inset.getInsetAccessor();
								String currentMethodReference = toMethodReferenceString(currentMethod);
								throw new MappingConfigurationException("Error while mapping "
										+ currentMethodReference + " : " + accessorDefinition.toString()
										+ " conflicts with " + AccessorDefinition.toString(existingMapping.getKey()) + " because they use same " 
										+ "column" +
										", override one of their name to avoid the conflict" +
										", see " + MethodReferences.toMethodReferenceString(
										(SerializableTriFunction<EmbedOptions, SerializableFunction, String, EmbedOptions>) EmbedOptions::overrideName));
							}
							
							AccessorChain chain = newAccessorChain(rootAccessors, valueAccessPoint);
							result.put(chain, targetColumn);
						});
			} else if (inset instanceof FluentEmbeddableMappingConfigurationSupport.ImportedInset) {
				EmbeddableMappingConfiguration<?> configuration = ((ImportedInset) inset).getBeanMappingBuilder().getConfiguration();
				includeDirectMapping(configuration, inset.getAccessor(), inset.getOverridenColumnNames(), ((ImportedInset<?, ?>) inset).getOverridenColumns(),
						inset.getExcludedProperties());
				stack.addAll(configuration.getInsets());
			}
			treatedInsets.add(inset);
		}
	}
	
	@VisibleForTesting
	static AccessorChain newAccessorChain(List<ValueAccessPoint> rootAccessors, ValueAccessPoint terminalAccessor) {
		// we must clone rootAccessors to prevent from accessor mixing
		List<ValueAccessPoint> accessorsTmp = new ArrayList<>(rootAccessors);
		accessorsTmp.add(terminalAccessor);
		List<IAccessor> finalAccessors = new ArrayList<>();
		accessorsTmp.forEach(valueAccessPoint -> {
					if (valueAccessPoint instanceof IAccessor) {
						finalAccessors.add((IAccessor) valueAccessPoint);
					} else if (valueAccessPoint instanceof IReversibleMutator) {
						finalAccessors.add(((IReversibleMutator) valueAccessPoint).toAccessor());
					} else {
						// Something has change in ValueAccessPoint hierarchy, it should be fixed (grave)
						throw new NotImplementedException(Reflections.toString(valueAccessPoint.getClass()) + " is unknown from chain creation algorithm");
					}
				});
		return AccessorChain.forModel(finalAccessors);
	}
	
	private Set<ValueAccessPointByMethod> giveMappableMethods(Inset<?, ?> inset, Set<ValueAccessPoint> innerEmbeddableRegistry) {
		InstanceMethodIterator methodIterator = new InstanceMethodIterator(inset.getEmbeddedClass(), Object.class);
		ValueAccessPointSet excludedProperties = inset.getExcludedProperties();
		// NB: we skip fields that are registered as inset (inner embeddable) because they'll be treated by their own, skipping them avoids conflicts
		Predicate<ValueAccessPoint> innerEmbeddableExcluder = not(innerEmbeddableRegistry::contains);
		// we also skip excluded fields by user
		Predicate<ValueAccessPoint> excludedFieldsExcluder = not(excludedProperties::contains);
		
		// we use a ValueAccessPointSet to keep only one of a getter or a setter found for the same property (because MethodIterator iterates both)
		ValueAccessPointSet result = new ValueAccessPointSet();
		methodIterator.forEachRemaining(m -> {
			ValueAccessPointByMethod valueAccessPoint = Reflections.onJavaBeanPropertyWrapperNameGeneric(m.getName(), m,
					AccessorByMethod::new,
					MutatorByMethod::new,
					AccessorByMethod::new,
					method -> null /* non Java Bean naming convention compliant method */);
			if (valueAccessPoint != null && innerEmbeddableExcluder.and(excludedFieldsExcluder).test(valueAccessPoint)) {
				result.add(valueAccessPoint);
			}
		});

		return (Set) result; 
	}
	
	/**
	 * Ensures that a type is not already embedded, because its columns would conflict with already defined ones, or checks that every property
	 * is overriden.
	 * Throws an exception if that's not the case.
	 * 
	 * @param inset current inset to be checked for duplicate
	 * @param treatedInsets already mapped insets : if one of them matches given inset on mapped type, then a fine-graned check is done to look for conflict
	 */
	private void assertNotAlreadyDeclared(AbstractInset<?, ?> inset, Set<AbstractInset<?, ?>> treatedInsets) {
		Optional<AbstractInset<?, ?>> alreadyMappedType = treatedInsets.stream().filter(i -> i.getEmbeddedClass() == inset.getEmbeddedClass()).findFirst();
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
					nullable((ValueAccessPoint) Reflections.onJavaBeanPropertyWrapperNameGeneric(m.getName(), m,
							AccessorByMethod::new,
							MutatorByMethod::new,
							AccessorByMethod::new,
							method -> null)).invoke(expectedOverridenFields::add)
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
									.stream().map(AccessorDefinition::toString).collect(Collectors.joining(", ")));
				}
			}
		}
	}
	
	/**
	 * {@link Iterator} dedicated to {@link Inset} in ascending order : if "Toto::getTata::getTiti::getTutu" is defined then Titi::getTutu,
	 * Tata::getTiti, Toto::getTata is returned
	 */
	private static class InsetChainIterator implements Iterator<Inset> {
		
		private Inset current;
		
		private InsetChainIterator(Inset starter) {
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
	
	/**
	 * Wrapper to give {@link Column} name according to given {@link ColumnNamingStrategy} if present.
	 * If absent {@link ColumnNamingStrategy#DEFAULT} will be used.
	 */
	static class ColumnNameProvider {
		
		private final ColumnNamingStrategy columnNamingStrategy;
		
		ColumnNameProvider(@Nullable ColumnNamingStrategy columnNamingStrategy) {
			this.columnNamingStrategy = nullable(columnNamingStrategy).getOr(ColumnNamingStrategy.DEFAULT);
		}
		
		protected String giveColumnName(Linkage linkage) {
			return nullable(linkage.getColumnName())
					.getOr(() -> giveColumnName(AccessorDefinition.giveDefinition(linkage.getAccessor())));
		}
		
		protected String giveColumnName(AccessorDefinition accessorDefinition) {
			return columnNamingStrategy.giveName(accessorDefinition);
		}
	}
	
	static class DuplicateDefinitionChecker implements Consumer<Linkage> {
		
		private final String columnNameToCheck;
		private final IReversibleAccessor propertyAccessor;
		private final ColumnNameProvider columnNameProvider;
		private final ValueAccessPointComparator valueAccessPointComparator = new ValueAccessPointComparator();
		
		DuplicateDefinitionChecker(IReversibleAccessor propertyAccessor, String columnNameToCheck, ColumnNameProvider columnNameProvider) {
			this.columnNameToCheck = columnNameToCheck;
			this.propertyAccessor = propertyAccessor;
			this.columnNameProvider = columnNameProvider;
		}
		@Override
		public void accept(Linkage pawn) {
			IReversibleAccessor accessor = pawn.getAccessor();
			if (valueAccessPointComparator.compare(accessor, propertyAccessor) == 0) {
				throw new MappingConfigurationException("Mapping is already defined by method " + AccessorDefinition.toString(propertyAccessor));
			} else if (columnNameToCheck.equals(nullable(pawn.getColumnName()).getOr(() -> columnNameProvider.giveColumnName(pawn)))) {
				throw new MappingConfigurationException("Column '" + columnNameToCheck + "' of mapping '" + AccessorDefinition.toString(propertyAccessor)
						+ "' is already targetted by '" + AccessorDefinition.toString(pawn.getAccessor()) + "'");
			}
		}
	}
}
