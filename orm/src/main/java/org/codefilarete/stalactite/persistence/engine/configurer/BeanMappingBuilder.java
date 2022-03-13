package org.codefilarete.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointComparator;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.persistence.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.persistence.engine.MappingConfigurationException;
import org.codefilarete.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;
import static org.codefilarete.reflection.MethodReferences.toMethodReferenceString;

/**
 * Engine that converts mapping definition of a {@link EmbeddableMappingConfiguration} to a simple {@link Map}.
 * Designed as such as its instances can be reused (no constructor, attributes are set through
 * {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}
 * therefore they are not thread-safe. This was made to avoid extra instance creation because this class is used in some loops.
 * 
 * Whereas it consumes an {@link EmbeddableMappingConfiguration} it doesn't mean that its goal is to manage embedded beans of an entity : as its
 * name says it's aimed at collecting mapping of any beans, without the entity part (understanding identification and inheritance which is targetted
 * by {@link PersisterBuilderImpl})
 * 
 * @author Guillaume Mary
 * @see #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)    
 */
class BeanMappingBuilder {
	
	/**
	 * Iterates over configuration to look for any property defining a {@link Column} to use, its table would be the one to be used by builder.
	 * Throws an exception if several different tables are found during iteration.
	 * 
	 * @param mappingConfiguration the configuration to look up for any oerriding {@link Column}
	 * @return null if no {@link Table} was found (meaning that builder is free to create one)
	 */
	@VisibleForTesting
	static Table giveTargetTable(EmbeddableMappingConfiguration<?> mappingConfiguration) {
		Holder<Table> result = new Holder<>();
		
		// algorithm close to the one of includeEmbeddedMapping(..)
		Queue<Inset> stack = new ArrayDeque<>(mappingConfiguration.getInsets());
		while (!stack.isEmpty()) {
			Inset<?, ?> inset = stack.poll();
			inset.getOverridenColumns().forEach((valueAccessPoint, targetColumn) ->
				assertHolderIsFilledWithTargetTable(result, targetColumn, valueAccessPoint)
			);
			EmbeddableMappingConfiguration<?> configuration = inset.getBeanMappingBuilder().getConfiguration();
			stack.addAll(configuration.getInsets());
		}
		return result.get();
	}
	
	private static void assertHolderIsFilledWithTargetTable(Holder<Table> result, Column targetColumn, ValueAccessPoint valueAccessPoint) {
		if (targetColumn != null) {
			if (result.get() != null && result.get() != targetColumn.getTable()) {
				throw new MappingConfigurationException("Property override doesn't target main table : " + valueAccessPoint);
			}
			result.set(targetColumn.getTable());
		}
	}
	
	private EmbeddableMappingConfiguration<?> mappingConfiguration;
	private Table targetTable;
	private ColumnNameProvider columnNameProvider;
	private ColumnBinderRegistry columnBinderRegistry;
	/** Result of {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}, shared between methods */
	private Map<ReversibleAccessor, Column> result;
	
	/**
	 * Converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a {@link Map} representing the definition of the mapping declared by the {@link EmbeddableMappingConfiguration}
	 */
	Map<ReversibleAccessor, Column> build(EmbeddableMappingConfiguration mappingConfiguration,
										  Table targetTable,
										  ColumnBinderRegistry columnBinderRegistry,
										  ColumnNameProvider columnNameProvider) {
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
	
	protected void includeDirectMapping(EmbeddableMappingConfiguration<?> givenConfiguration,
										@Nullable ValueAccessPoint accessorPrefix,
										ValueAccessPointMap<String> overridenColumnNames,
										ValueAccessPointMap<Column> overridenColumns,
										ValueAccessPointSet excludedProperties) {
		givenConfiguration.getPropertiesMapping().stream()
				.filter(linkage -> !excludedProperties.contains(linkage.getAccessor()))
				.forEach(linkage -> includeMapping(linkage, givenConfiguration, accessorPrefix,
						overridenColumnNames.get(linkage.getAccessor()),
						overridenColumns.get(linkage.getAccessor())));
	}
	
	protected <C, O> void includeMapping(Linkage<C, O> linkage,
								  EmbeddableMappingConfiguration<?> mappingConfiguration,
								  @Nullable ValueAccessPoint accessorPrefix,
								  @Nullable String overridenColumnName,
								  @Nullable Column<Table, O> overridenColumn) {
		String columnName = nullable(overridenColumn).map(Column::getName).getOr(() -> determineColumnName(linkage, overridenColumnName));
		assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
		Column<Table, O> column = nullable(overridenColumn).getOr(() -> addColumnToTable(linkage, columnName));
		ensureColumnBindingInRegistry(linkage, column);
		ReversibleAccessor accessor;
		if (accessorPrefix != null) {
			accessor = newAccessorChain(Collections.singletonList(accessorPrefix), linkage.getAccessor(), mappingConfiguration.getBeanType());
		} else {
			accessor = linkage.getAccessor();
		}
		result.put(accessor, column);
	}
	
	protected <C, O> void assertMappingIsNotAlreadyDefinedByInheritance(Linkage<C, O> linkage, String columnNameToCheck, EmbeddableMappingConfiguration<?> mappingConfiguration) {
		DuplicateDefinitionChecker duplicateDefinitionChecker = new DuplicateDefinitionChecker(linkage.getAccessor(), columnNameToCheck, columnNameProvider);
		stream(mappingConfiguration.inheritanceIterable())
				.flatMap(configuration -> (Stream<Linkage>) configuration.getPropertiesMapping().stream())
				// not using equals() is voluntary since we want referencial checking here to exclude same instance, prevent equals() override to break this algorithm
				.filter(pawn -> linkage != pawn)
				.forEach(duplicateDefinitionChecker);
	}
	
	protected <C, O> Column<Table, O> addColumnToTable(Linkage<C, O> linkage, String columnName) {
		Column<Table, O> addedColumn = targetTable.addColumn(columnName, linkage.getColumnType());
		addedColumn.setNullable(linkage.isNullable());
		return addedColumn;
	}
	
	private <C, O> String determineColumnName(Linkage<C, O> linkage, @Nullable String overridenColumName) {
		return nullable(overridenColumName).elseSet(linkage.getColumnName()).getOr(() -> columnNameProvider.giveColumnName(linkage));
	}
	
	protected <C, O> void ensureColumnBindingInRegistry(Linkage<C, O> linkage, Column<?, O> column) {
		// assert that column binder is registered : it will throw en exception if the binder is not found
		if (linkage.getParameterBinder() != null) {
			columnBinderRegistry.register(column, linkage.getParameterBinder());
		}
	}
	
	/**
	 * Creates equivalent {@link Column}s of those of given {@link Map} values on the given target table. May do nothing if columns already exist.
	 *
	 * @param propertyToColumn mapping between some property accessors and {@link Column}s from default target {@link Table}
	 * 							(the one given at {@link #build(EmbeddableMappingConfiguration, Table, ColumnBinderRegistry, ColumnNameProvider)}
	 * 							time)
	 * @param localTargetTable the table on which columns must be added
	 * @return a mapping between properties of given {@link Map} keys and their column in given {@link Table}
	 */
	protected static Map<ReversibleAccessor, Column> projectColumns(Map<? extends ReversibleAccessor, ? extends Column> propertyToColumn,
																	Table localTargetTable,
																	BiFunction<ReversibleAccessor, Column, String> columnNameSupplier) {
		Map<ReversibleAccessor, Column> localResult = new HashMap<>();
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
		Set<Inset<?, ?>> treatedInsets = new HashSet<>();
		
		Queue<Inset> stack = Collections.asLifoQueue(new ArrayDeque<>());
		stack.addAll(mappingConfiguration.getInsets());
		Queue<Accessor> accessorPath = new ArrayDeque<>();
		while (!stack.isEmpty()) {
			Inset<?, ?> inset = stack.poll();
			
			assertNotAlreadyDeclared(inset, treatedInsets);
			
			EmbeddableMappingConfiguration<?> configuration = inset.getBeanMappingBuilder().getConfiguration();
			ValueAccessPoint mappingPrefix = null;
			if (inset.getAccessor() != null) {
				accessorPath.add(inset.getAccessor());
				// small optimization to avoid creation of an Accessor chain of 1 element
				if (accessorPath.size() == 1) {
					mappingPrefix = accessorPath.peek();
				} else {
					mappingPrefix = AccessorChain.forModel(new ArrayList<>(accessorPath));
				}
			}
			EmbeddableMappingConfiguration<?> superClassConfiguration = configuration.getMappedSuperClassConfiguration();
			if (superClassConfiguration != null) {
				includeMappedSuperClassMapping(inset, accessorPath, superClassConfiguration);
			}
			
			includeDirectMapping(configuration, mappingPrefix, inset.getOverridenColumnNames(), inset.getOverridenColumns(),
					inset.getExcludedProperties());
			if (configuration.getInsets().isEmpty()) {
				accessorPath.remove();
			} else {
				stack.addAll(configuration.getInsets());
			}
			treatedInsets.add(inset);
		}
	}
	
	private void includeMappedSuperClassMapping(Inset<?, ?> inset, Collection<Accessor> accessorPath, EmbeddableMappingConfiguration<?> superClassConfiguration) {
		// we include super type mapping by using a new instance of BeanMappingBuilder, this is the simplest (but maybe not the most
		// debuggable) and allows to manage inheritance of several mappedSuperClass 
		Map<ReversibleAccessor, Column> superMapping = new BeanMappingBuilder().build(superClassConfiguration, targetTable,
				this.columnBinderRegistry, this.columnNameProvider);
		Class<?> insetBeanType = inset.getBeanMappingBuilder().getConfiguration().getBeanType();
		superMapping.forEach((accessor, column) -> {
			AccessorChain prefix;
			List<Accessor> accessors;
			if (accessorPath.size() == 1) {
				accessors = Arrays.asList(Iterables.first(accessorPath), accessor);
			} else {
				accessors = new ArrayList<>(accessorPath);
				accessors.add(accessor);
			}
			prefix = AccessorChain.forModel(
				accessors,
				// this can look superfluous but fills the gap of instanciating right bean when configuration is a subtype of inset accessor,
				// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
				// (where T is type returned by accessor, or expected as input of mutator)
				(localAccessor, accessorInputType) -> insetBeanType);
			
			// Computing definitive column because it may be overriden by inset declaration
			Column finalColumn;
			if (inset.getOverridenColumns().containsKey(accessor)) {
				finalColumn = inset.getOverridenColumns().get(accessor);
			} else if (inset.getOverridenColumnNames().containsKey(accessor)) {
				finalColumn = targetTable.addColumn(inset.getOverridenColumnNames().get(accessor), column.getJavaType());
			} else {
				finalColumn = targetTable.addColumn(column.getName(), column.getJavaType());
			}
			result.put(prefix, finalColumn);
		});
	}
	
	private static AccessorChain newAccessorChain(List<ValueAccessPoint> rootAccessors, ValueAccessPoint terminalAccessor, Class<?> beanType) {
		// we must clone rootAccessors to prevent from accessor mixing
		List<ValueAccessPoint> accessorsTmp = new ArrayList<>(rootAccessors);
		accessorsTmp.add(terminalAccessor);
		List<Accessor> finalAccessors = new ArrayList<>();
		accessorsTmp.forEach(valueAccessPoint -> {
					if (valueAccessPoint instanceof Accessor) {
						finalAccessors.add((Accessor) valueAccessPoint);
					} else if (valueAccessPoint instanceof ReversibleMutator) {
						finalAccessors.add(((ReversibleMutator) valueAccessPoint).toAccessor());
					} else {
						// Something has change in ValueAccessPoint hierarchy, it should be fixed (grave)
						throw new NotImplementedException(Reflections.toString(valueAccessPoint.getClass()) + " is unknown from chain creation algorithm");
					}
				});
		return AccessorChain.forModel(
				finalAccessors,
				// this can look superfluous but fills the gap of instanciating right bean when configuration is a subtype of inset accessor,
				// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
				// (where T is type returned by accessor, or expected as input of mutator)
				(accessor, aClass) -> beanType);
	}
	
	/**
	 * Ensures that a bean is not already embedded with same accessor, because its columns would conflict with already defined ones, or checks that every property
	 * is overriden.
	 * Throws an exception if that's not the case.
	 * 
	 * @param inset current inset to be checked for duplicate
	 * @param treatedInsets already mapped insets : if one of them matches given inset on mapped type, then a fine-graned check is done to look for conflict
	 */
	private void assertNotAlreadyDeclared(Inset<?, ?> inset, Set<Inset<?, ?>> treatedInsets) {
		Optional<Inset<?, ?>> alreadyMappedType = treatedInsets.stream().filter(i -> i.getEmbeddedClass() == inset.getEmbeddedClass()).findFirst();
		if (alreadyMappedType.isPresent()) {
			// accessors are exactly the same ?
			if (alreadyMappedType.get().getInsetAccessor().equals(inset.getInsetAccessor())) {
				Method currentMethod = inset.getInsetAccessor();
				String currentMethodReference = toMethodReferenceString(currentMethod);
				throw new MappingConfigurationException(currentMethodReference + " is already mapped");
			}
			
			Map<String, ValueAccessPoint> columNamePerAccessPoint1 = new HashMap<>();
			inset.getBeanMappingBuilder().getConfiguration().getPropertiesMapping().forEach(linkage -> {
				if (!inset.getExcludedProperties().contains(linkage.getAccessor())) {
					String columnName = determineColumnName(linkage, inset.getOverridenColumnNames().get(linkage.getAccessor()));
					columNamePerAccessPoint1.put(columnName, linkage.getAccessor());
				}
			});
			Inset<?, ?> abstractInset = alreadyMappedType.get();
			Map<String, ValueAccessPoint> columNamePerAccessPoint2 = new HashMap<>();
			inset.getBeanMappingBuilder().getConfiguration().getPropertiesMapping().forEach(linkage -> {
				if (!abstractInset.getExcludedProperties().contains(linkage.getAccessor())) {
					String columnName = determineColumnName(linkage, abstractInset.getOverridenColumnNames().get(linkage.getAccessor()));
					columNamePerAccessPoint2.put(columnName, linkage.getAccessor());
				}
			});
			Map<ValueAccessPoint, ValueAccessPoint> join = Maps.innerJoin(columNamePerAccessPoint1,
					columNamePerAccessPoint2);
			if (!join.isEmpty()) {
				String currentMethodReference = toMethodReferenceString(inset.getInsetAccessor());
				String conflictingDeclaration = toMethodReferenceString(abstractInset.getInsetAccessor());
				throw new MappingConfigurationException(
						currentMethodReference + " conflicts with " + conflictingDeclaration + " while embedding a " + Reflections.toString(inset.getEmbeddedClass())
								+ ", column names should be overriden : "
								+ join.keySet()
								.stream().map(AccessorDefinition::toString).collect(Collectors.joining(", ")));
			}
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
		private final ReversibleAccessor propertyAccessor;
		private final ColumnNameProvider columnNameProvider;
		private static final ValueAccessPointComparator VALUE_ACCESS_POINT_COMPARATOR = new ValueAccessPointComparator();
		
		DuplicateDefinitionChecker(ReversibleAccessor propertyAccessor, String columnNameToCheck, ColumnNameProvider columnNameProvider) {
			this.columnNameToCheck = columnNameToCheck;
			this.propertyAccessor = propertyAccessor;
			this.columnNameProvider = columnNameProvider;
		}
		@Override
		public void accept(Linkage pawn) {
			ReversibleAccessor accessor = pawn.getAccessor();
			if (VALUE_ACCESS_POINT_COMPARATOR.compare(accessor, propertyAccessor) == 0) {
				throw new MappingConfigurationException("Mapping is already defined by method " + AccessorDefinition.toString(propertyAccessor));
			} else if (columnNameToCheck.equals(nullable(pawn.getColumnName()).getOr(() -> columnNameProvider.giveColumnName(pawn)))) {
				throw new MappingConfigurationException("Column '" + columnNameToCheck + "' of mapping '" + AccessorDefinition.toString(propertyAccessor)
						+ "' is already targetted by '" + AccessorDefinition.toString(pawn.getAccessor()) + "'");
			}
		}
	}
}
