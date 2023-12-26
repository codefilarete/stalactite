package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointComparator;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMappingConfiguration.Inset;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMappingConfiguration.Linkage;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.collection.StreamSplitter;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Predicates;

import static org.codefilarete.reflection.MethodReferences.toMethodReferenceString;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * Engine that converts mapping definition of a {@link BeanMappingConfiguration} to a simple {@link Map}.
 * Designed as such as its instances can be reused (no constructor, attributes are set through
 * {@link #build()}
 * therefore they are not thread-safe. This was made to avoid extra instance creation because this class is used in some loops.
 * 
 * Whereas it consumes an {@link BeanMappingConfiguration} it doesn't mean that its goal is to manage embedded beans of an entity : as its
 * name says it's aimed at collecting mapping of any beans, without the entity part (understanding identification and inheritance which is targeted
 * by {@link PersisterBuilderImpl})
 * 
 * @author Guillaume Mary
 * @see #build()    
 */
public class BeanMappingBuilder<C, T extends Table<T>> {
	
	/**
	 * Iterates over configuration to look for any property defining a {@link Column} to use, its table would be the one to be used by builder.
	 * Throws an exception if several tables are found during iteration.
	 * 
	 * @param mappingConfiguration the configuration to look up for any overriding {@link Column}
	 * @return null if no {@link Table} was found (meaning that builder is free to create one)
	 */
	public static Table giveTargetTable(EmbeddableMappingConfiguration<?> mappingConfiguration) {
		Holder<Table> result = new Holder<>();
		
		// algorithm close to the one of includeEmbeddedMapping(..)
		Queue<Inset> stack = new ArrayDeque<>(BeanMappingConfiguration.fromEmbeddableMappingConfiguration(mappingConfiguration).getInsets());
		while (!stack.isEmpty()) {
			Inset<?, ?> inset = stack.poll();
			inset.getOverriddenColumns().forEach((valueAccessPoint, targetColumn) ->
				assertHolderIsFilledWithTargetTable(result, targetColumn, valueAccessPoint)
			);
			BeanMappingConfiguration<?> configuration = inset.getConfiguration();
			stack.addAll(configuration.getInsets());
		}
		return result.get();
	}
	
	private static void assertHolderIsFilledWithTargetTable(Holder<Table> result, Column targetColumn, ValueAccessPoint<?> valueAccessPoint) {
		if (targetColumn != null) {
			if (result.get() != null && result.get() != targetColumn.getTable()) {
				throw new MappingConfigurationException("Property override doesn't target main table : " + valueAccessPoint);
			}
			result.set(targetColumn.getTable());
		}
	}
	
	/**
	 * Creates equivalent {@link Column}s of those given in {@link Map} values onto given target table. May do nothing if columns already exist.
	 *
	 * @param propertyToColumn mapping between some property accessors and {@link Column}s from default target {@link Table}
	 * (the one given at {@link #build()} time)
	 * @param targetTable the table on which columns must be added
	 * @return a mapping between properties of given {@link Map} keys and their column in given {@link Table}
	 */
	public static <C, T extends Table<T>, A extends ValueAccessPoint<C>> Map<A, Column<T, Object>>
	projectColumns(Map<? extends A, ? extends Column<?, Object>> propertyToColumn,
				   T targetTable,
				   BiFunction<? super A, Column<?, Object>, String> columnNameSupplier) {
		Map<A, Column<T, Object>> localResult = new HashMap<>();
		propertyToColumn.forEach((accessor, column) -> {
			Column<T, Object> projectedColumn = targetTable.addColumn(columnNameSupplier.apply(accessor, column), column.getJavaType());
			projectedColumn.setAutoGenerated(column.isAutoGenerated());
			projectedColumn.setNullable(column.isNullable());
			localResult.put(accessor, projectedColumn);
		});
		return localResult;
	}
	
	/**
	 * Composes an {@link AccessorChain} from given {@link ValueAccessPoint}.
	 * Acts as a caster or converter because {@link ValueAccessPoint} is an abstract vision of elements accepted by
	 * {@link AccessorChain}.
	 * Supported {@link ValueAccessPoint}s are {@link Accessor} and {@link ReversibleAccessor}, else an exception will be thrown
	 *
	 * @param rootAccessors first elements to access a property
	 * @param terminalAccessor final accessor that read property of latest element of rootAccessors
	 * @param beanType read property type
	 * @return a new {@link AccessorChain} that access property of terminalAccessor thanks to all accessors of rootAccessors
	 */
	private static <SRC, TRGT> AccessorChain<SRC, TRGT> newAccessorChain(List<? extends ValueAccessPoint<?>> rootAccessors,
																		 ValueAccessPoint<?> terminalAccessor,
																		 Class<TRGT> beanType) {
		// we must clone rootAccessors to prevent from accessor mixing
		List<ValueAccessPoint<?>> accessorsTmp = new ArrayList<>(rootAccessors);
		accessorsTmp.add(terminalAccessor);
		List<Accessor<?, ?>> finalAccessors = new ArrayList<>();
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
		return AccessorChain.chainNullSafe(
				finalAccessors,
				// this can look superfluous but fills the gap of instantiating right bean when configuration is a subtype of inset accessor,
				// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
				// (where T is type returned by accessor, or expected as input of mutator)
				(accessor, aClass) -> beanType);
	}
	
	private final BeanMappingConfiguration<C> mainMappingConfiguration;
	private final T targetTable;
	private final ColumnNameProvider columnNameProvider;
	private final ColumnBinderRegistry columnBinderRegistry;
	
	public BeanMappingBuilder(EmbeddableMappingConfiguration<C> mappingConfiguration,
							  T targetTable,
							  ColumnBinderRegistry columnBinderRegistry,
							  ColumnNameProvider columnNameProvider) {
		this(BeanMappingConfiguration.fromEmbeddableMappingConfiguration(mappingConfiguration),
				targetTable,
				columnBinderRegistry,
				columnNameProvider);
	}
	
	public BeanMappingBuilder(EmbeddableMappingConfiguration<C> mappingConfiguration,
							  T targetTable,
							  ColumnBinderRegistry columnBinderRegistry,
							  ColumnNamingStrategy columnNamingStrategy) {
		this(BeanMappingConfiguration.fromEmbeddableMappingConfiguration(mappingConfiguration),
				targetTable,
				columnBinderRegistry,
				new ColumnNameProvider(columnNamingStrategy));
	}
	
	public BeanMappingBuilder(CompositeKeyMappingConfiguration<C> mappingConfiguration,
							  T targetTable,
							  ColumnBinderRegistry columnBinderRegistry,
							  ColumnNamingStrategy columnNamingStrategy) {
		this(BeanMappingConfiguration.fromCompositeKeyMappingConfiguration(mappingConfiguration),
				targetTable,
				columnBinderRegistry,
				new ColumnNameProvider(columnNamingStrategy));
	}
	
	private BeanMappingBuilder(BeanMappingConfiguration<C> mappingConfiguration,
							  T targetTable,
							  ColumnBinderRegistry columnBinderRegistry,
							  ColumnNameProvider columnNameProvider) {
		this.mainMappingConfiguration = mappingConfiguration;
		this.targetTable = targetTable;
		this.columnNameProvider = columnNameProvider;
		this.columnBinderRegistry = columnBinderRegistry;
	}
	
	/**
	 * Converts mapping definition of a {@link BeanMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a bean that stores some {@link Map}s representing the definition of the mapping declared by the {@link BeanMappingConfiguration}
	 */
	public BeanMapping<C, T> build() {
		return build(false);
	}
	
	public BeanMapping<C, T> build(boolean onlyExtraTableLinkages) {
		InternalProcessor internalProcessor = new InternalProcessor(onlyExtraTableLinkages);
		// converting direct mapping
		internalProcessor.includeDirectMapping(this.mainMappingConfiguration, null, new ValueAccessPointMap<>(), new ValueAccessPointMap<>(), new ValueAccessPointSet<>());
		// adding embeddable (no particular thought about order compared to previous direct mapping) 
		internalProcessor.includeEmbeddedMapping();
		return internalProcessor.result;
	}
	
	protected T getTargetTable() {
		return targetTable;
	}
	
	/**
	 * Internal engine driven by {@link #build()} method.
	 * Made to store result in another class than main one and decouple configuration from process.
	 * 
	 * @author Guillaume Mary
	 */
	private class InternalProcessor {
		
		private final BeanMapping<C, T> result = new BeanMapping<>();
		
		private final boolean onlyExtraTableLinkages;
		
		private InternalProcessor(boolean onlyExtraTableLinkages) {
			this.onlyExtraTableLinkages = onlyExtraTableLinkages;
		}
		
		protected void includeDirectMapping(BeanMappingConfiguration<?> mappingConfiguration,
											@Nullable ValueAccessPoint<C> accessorPrefix,
											ValueAccessPointMap<C, String> overriddenColumnNames,
											ValueAccessPointMap<C, Column<T, ?>> overriddenColumns,
											ValueAccessPointSet<C> excludedProperties) {
			Stream<Linkage> linkageStream = mappingConfiguration.getPropertiesMapping().stream()
					.filter(linkage -> !excludedProperties.contains(linkage.getAccessor()));
			
			if (!onlyExtraTableLinkages) {
				// this method (and class) doesn't deal with extra table
				linkageStream = linkageStream.filter(linkage -> linkage.getExtraTableName() == null);
			}
			new StreamSplitter<>(linkageStream)
					.dispatch(Predicates.not(Linkage::isReadonly),
							linkage -> {
								Column<T, ?> overriddenColumn = overriddenColumns.get(linkage.getAccessor());
								String columnName = nullable(overriddenColumn)
										.map(Column::getName)
										.getOr(() -> determineColumnName(linkage, overriddenColumnNames.get(linkage.getAccessor())));
								assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
								Duo<ReversibleAccessor<C, Object>, Column<T, Object>> mapping = includeMapping(linkage, accessorPrefix, columnName, overriddenColumn, mappingConfiguration.getBeanType());
								result.mapping.put(mapping.getLeft(), mapping.getRight());
							})
					.dispatch(Linkage::isReadonly,
							linkage -> {
								Column<T, ?> overriddenColumn = overriddenColumns.get(linkage.getAccessor());
								String columnName = nullable(overriddenColumn)
										.map(Column::getName)
										.getOr(() -> determineColumnName(linkage, overriddenColumnNames.get(linkage.getAccessor())));
								assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
								Duo<ReversibleAccessor<C, Object>, Column<T, Object>> mapping = includeMapping(linkage, accessorPrefix, columnName, overriddenColumn, mappingConfiguration.getBeanType());
								result.readonlyMapping.put(mapping.getLeft(), mapping.getRight());
							})
					.split();
		}
		
		protected <O> Duo<ReversibleAccessor<C, ?>, Column<T, Object>> includeMapping(Linkage<C, O> linkage,
																					  @Nullable ValueAccessPoint<C> accessorPrefix,
																					  String columnName,
																					  @Nullable Column<T, O> overriddenColumn,
																					  Class<?> embeddedBeanType) {
			Column<T, O> column = nullable(overriddenColumn).getOr(() -> addColumnToTable(linkage, columnName));
			ensureColumnBindingInRegistry(linkage, column);
			ReversibleAccessor<C, ?> accessor;
			if (accessorPrefix != null) {
				accessor = newAccessorChain(Collections.singletonList(accessorPrefix), linkage.getAccessor(), embeddedBeanType);
			} else {
				accessor = linkage.getAccessor();
			}
			return new Duo<>(accessor, (Column<T, Object>) column);
		}
		
		protected <O> void assertMappingIsNotAlreadyDefinedByInheritance(Linkage<C, O> linkage, String columnNameToCheck, BeanMappingConfiguration<O> mappingConfiguration) {
			DuplicateDefinitionChecker duplicateDefinitionChecker = new DuplicateDefinitionChecker(linkage.getAccessor(), columnNameToCheck, columnNameProvider);
			stream(mappingConfiguration.inheritanceIterable())
					.flatMap(configuration -> (Stream<Linkage>) configuration.getPropertiesMapping().stream())
					// not using equals() is voluntary since we want reference checking here to exclude same instance,
					// since given linkage is one of given mappingConfiguration
					// (doing as such also prevent equals() method override to break this algorithm)
					.filter(pawn -> linkage != pawn
							// only writable properties are concerned by this check : we allow duplicates for readonly properties
							&& !pawn.isReadonly() && !linkage.isReadonly())
					.forEach(duplicateDefinitionChecker);
		}
		
		protected <O> Column<T, O> addColumnToTable(Linkage<C, O> linkage, String columnName) {
			Column<T, O> addedColumn = targetTable.addColumn(columnName, linkage.getColumnType());
			addedColumn.setNullable(linkage.isNullable());
			return addedColumn;
		}
		
		private <O> String determineColumnName(Linkage<C, O> linkage, @Nullable String overriddenColumName) {
			return nullable(overriddenColumName).elseSet(linkage.getColumnName()).getOr(() -> columnNameProvider.giveColumnName(linkage));
		}
		
		protected <O> void ensureColumnBindingInRegistry(Linkage<C, O> linkage, Column<?, O> column) {
			// assert that column binder is registered : it will throw en exception if the binder is not found
			if (linkage.getParameterBinder() != null) {
				columnBinderRegistry.register(column, linkage.getParameterBinder());
			}
		}
		
		/**
		 * Adds embedded beans mapping to result
		 */
		private void includeEmbeddedMapping() {
			Set<Inset<C, ?>> treatedInsets = new HashSet<>();
			
			Queue<Inset<C, ?>> stack = Collections.asLifoQueue(new ArrayDeque<>());
			stack.addAll(mainMappingConfiguration.getInsets());
			Queue<Accessor<C, ?>> accessorPath = new ArrayDeque<>();
			while (!stack.isEmpty()) {
				Inset<C, ?> inset = stack.poll();
				
				assertNotAlreadyDeclared(inset, treatedInsets);
				
				BeanMappingConfiguration<C> configuration = (BeanMappingConfiguration<C>) inset.getConfiguration();
				ValueAccessPoint<C> mappingPrefix = null;
				if (inset.getAccessor() != null) {
					accessorPath.add(inset.getAccessor());
					// small optimization to avoid creation of an Accessor chain of 1 element
					if (accessorPath.size() == 1) {
						mappingPrefix = accessorPath.peek();
					} else {
						mappingPrefix = AccessorChain.chainNullSafe(new ArrayList<>(accessorPath));
					}
				}
				BeanMappingConfiguration<?> superClassConfiguration = configuration.getMappedSuperClassConfiguration();
				if (superClassConfiguration != null) {
					includeMappedSuperClassMapping(inset, accessorPath, superClassConfiguration);
				}
				
				includeDirectMapping(configuration, mappingPrefix, inset.getOverriddenColumnNames(), (ValueAccessPointMap) inset.getOverriddenColumns(),
						inset.getExcludedProperties());
				if (configuration.getInsets().isEmpty()) {
					accessorPath.remove();
				} else {
					stack.addAll(configuration.getInsets());
				}
				treatedInsets.add(inset);
			}
		}
		
		private void includeMappedSuperClassMapping(Inset<C, ?> inset, Collection<Accessor<C, ?>> accessorPath, BeanMappingConfiguration<?> superClassConfiguration) {
			// we include super type mapping by using a new instance of BeanMappingBuilder, this is the simplest (but maybe not the most
			// debuggable) and allows to manage inheritance of several mappedSuperClass 
			BeanMapping<C, T> superMapping = new BeanMappingBuilder<>((BeanMappingConfiguration<C>) superClassConfiguration, targetTable,
					columnBinderRegistry, columnNameProvider).build();
			Class<?> insetBeanType = inset.getConfiguration().getBeanType();
			superMapping.mapping.forEach((accessor, column) -> {
				AccessorChain prefix;
				List<Accessor<?, ?>> accessors;
				if (accessorPath.size() == 1) {
					accessors = Arrays.asList(Iterables.first(accessorPath), accessor);
				} else {
					accessors = new ArrayList<>(accessorPath);
					accessors.add(accessor);
				}
				prefix = AccessorChain.chainNullSafe(
						accessors,
						// this can look superfluous but fills the gap of instantiating right bean when configuration is a subtype of inset accessor,
						// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
						// (where T is type returned by accessor, or expected as input of mutator)
						(localAccessor, accessorInputType) -> insetBeanType);
				
				// Computing definitive column because it may be overridden by inset declaration
				Column<T, ?> finalColumn;
				if (inset.getOverriddenColumns().containsKey(accessor)) {
					finalColumn = inset.getOverriddenColumns().get(accessor);
				} else if (inset.getOverriddenColumnNames().containsKey(accessor)) {
					finalColumn = targetTable.addColumn(inset.getOverriddenColumnNames().get(accessor), column.getJavaType());
				} else {
					finalColumn = targetTable.addColumn(column.getName(), column.getJavaType());
				}
				result.mapping.put((ReversibleAccessor<C, Object>) prefix, (Column<T, Object>) finalColumn);
			});
		}
		
		
		/**
		 * Ensures that a bean is not already embedded with same accessor, because its columns would conflict with already defined ones, or checks that every property
		 * is overridden.
		 * Throws an exception if that's not the case.
		 *
		 * @param inset current inset to be checked for duplicate
		 * @param treatedInsets already mapped insets : if one of them matches given inset on mapped type, then a fine-graned check is done to look for conflict
		 */
		private void assertNotAlreadyDeclared(Inset<C, ?> inset, Set<Inset<C, ?>> treatedInsets) {
			Optional<Inset<C, ?>> alreadyMappedType = treatedInsets.stream().filter(i -> i.getEmbeddedClass() == inset.getEmbeddedClass()).findFirst();
			if (alreadyMappedType.isPresent()) {
				// accessors are exactly the same ?
				if (alreadyMappedType.get().getInsetAccessor().equals(inset.getInsetAccessor())) {
					Method currentMethod = inset.getInsetAccessor();
					String currentMethodReference = toMethodReferenceString(currentMethod);
					throw new MappingConfigurationException(currentMethodReference + " is already mapped");
				}
				
				Map<String, ValueAccessPoint<?>> columNamePerAccessPoint = new HashMap<>();
				BeanMappingConfiguration<?> insetConfiguration = inset.getConfiguration();
				insetConfiguration.getPropertiesMapping().forEach(linkage -> {
					if (!inset.getExcludedProperties().contains(linkage.getAccessor())) {
						String columnName = determineColumnName(linkage, inset.getOverriddenColumnNames().get(linkage.getAccessor()));
						columNamePerAccessPoint.put(columnName, linkage.getAccessor());
					}
				});
				Inset<?, ?> abstractInset = alreadyMappedType.get();
				Map<String, ValueAccessPoint<?>> columNamePerAccessPoint2 = new HashMap<>();
				insetConfiguration.getPropertiesMapping().forEach(linkage -> {
					if (!abstractInset.getExcludedProperties().contains(linkage.getAccessor())) {
						String columnName = determineColumnName(linkage, abstractInset.getOverriddenColumnNames().get(linkage.getAccessor()));
						columNamePerAccessPoint2.put(columnName, linkage.getAccessor());
					}
				});
				Map<ValueAccessPoint<?>, ValueAccessPoint<?>> join = Maps.innerJoin(columNamePerAccessPoint, columNamePerAccessPoint2);
				if (!join.isEmpty()) {
					String currentMethodReference = toMethodReferenceString(inset.getInsetAccessor());
					String conflictingDeclaration = toMethodReferenceString(abstractInset.getInsetAccessor());
					throw new MappingConfigurationException(
							currentMethodReference + " conflicts with " + conflictingDeclaration + " while embedding a " + Reflections.toString(inset.getEmbeddedClass())
									+ ", column names should be overridden : "
									+ join.keySet()
									.stream().map(AccessorDefinition::toString).collect(Collectors.joining(", ")));
				}
			}
		}
	}
	
	/**
	 * Resulting class of {@link #build()} process
	 * @param <C>
	 * @param <T>
	 */
	public static class BeanMapping<C, T extends Table<T>> {
		
		private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mapping = new HashMap<>();
		
		private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> readonlyMapping = new HashMap<>();
		
		/**
		 * @return mapped properties
		 */
		public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getMapping() {
			return mapping;
		}
		
		/**
		 * @return mapped readonly properties
		 */
		public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getReadonlyMapping() {
			return readonlyMapping;
		}
	}
	
	/**
	 * Wrapper to give {@link Column} name according to given {@link ColumnNamingStrategy} if present.
	 * If absent {@link ColumnNamingStrategy#DEFAULT} will be used.
	 */
	public static class ColumnNameProvider {
		
		private final ColumnNamingStrategy columnNamingStrategy;
		
		public ColumnNameProvider(@Nullable ColumnNamingStrategy columnNamingStrategy) {
			this.columnNamingStrategy = nullable(columnNamingStrategy).getOr(ColumnNamingStrategy.DEFAULT);
		}
		
		protected String giveColumnName(Linkage linkage) {
			return nullable(linkage.getColumnName())
					.elseSet(nullable(linkage.getField()).map(Field::getName))
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
						+ "' is already targeted by '" + AccessorDefinition.toString(pawn.getAccessor()) + "'");
			}
		}
	}
	
	public interface BeanMappingConfiguration<C> {
		
		static <C> BeanMappingConfiguration<C> fromCompositeKeyMappingConfiguration(CompositeKeyMappingConfiguration<C> compositeKeyMappingConfiguration) {
			return new BeanMappingConfiguration<C>() {
				
				private final List<Linkage> linkages = Iterables.collectToList(compositeKeyMappingConfiguration.getPropertiesMapping(), embeddableLinkage -> new Linkage() {
					@Override
					public ReversibleAccessor getAccessor() {
						return embeddableLinkage.getAccessor();
					}
					
					@Nullable
					@Override
					public Field getField() {
						// this code serves little purpose since mapKey() doesn't give access to field override, maybe Linkage
						// contract should be specialized for Key mapping.
						return embeddableLinkage.getField();
					}
					
					@Nullable
					@Override
					public String getColumnName() {
						return embeddableLinkage.getColumnName();
					}
					
					@Override
					public Class getColumnType() {
						return embeddableLinkage.getColumnType();
					}
					
					@Override
					public String getExtraTableName() {
						// we have to return null here since this method is used by BeanMappingBuilder to keep
						// properties of main table
						// TODO : a better Linkage API
						return null;
					}
					
					@Override
					public ParameterBinder getParameterBinder() {
						return embeddableLinkage.getParameterBinder();
					}
					
					@Override
					public boolean isNullable() {
						return false;
					}
					
					@Override
					public boolean isReadonly() {
						return false;
					}
				});
				
				@Override
				public Class<C> getBeanType() {
					return compositeKeyMappingConfiguration.getBeanType();
				}
				
				@Override
				@Nullable
				public BeanMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
					return org.codefilarete.tool.Nullable.nullable(compositeKeyMappingConfiguration.getMappedSuperClassConfiguration())
							.map(BeanMappingConfiguration::fromCompositeKeyMappingConfiguration)
							.get();
				}
				
				@Override
				public List<Linkage> getPropertiesMapping() {
					return linkages;
				}
				
				@Override
				public Collection<Inset<C, Object>> getInsets() {
					return Iterables.collectToList(compositeKeyMappingConfiguration.getInsets(), compositeInset -> new Inset<C, Object>() {
						@Override
						public PropertyAccessor<C, Object> getAccessor() {
							return compositeInset.getAccessor();
						}
						
						@Override
						public Method getInsetAccessor() {
							return compositeInset.getInsetAccessor();
						}
						
						@Override
						public Class<Object> getEmbeddedClass() {
							return compositeInset.getEmbeddedClass();
						}
						
						@Override
						public ValueAccessPointSet<C> getExcludedProperties() {
							return compositeInset.getExcludedProperties();
						}
						
						@Override
						public ValueAccessPointMap<C, String> getOverriddenColumnNames() {
							return compositeInset.getOverriddenColumnNames();
						}
						
						@Override
						public ValueAccessPointMap<C, Column> getOverriddenColumns() {
							return compositeInset.getOverriddenColumns();
						}
						
						@Override
						public BeanMappingConfiguration<Object> getConfiguration() {
							return fromCompositeKeyMappingConfiguration(compositeInset.getConfigurationProvider().getConfiguration());
						}
					});
				}
			};
		}
		
		static <C> BeanMappingConfiguration<C> fromEmbeddableMappingConfiguration(EmbeddableMappingConfiguration<C> embeddableMappingConfiguration) {
			return new BeanMappingConfiguration<C>() {
				
				private final List<Linkage> linkages = Iterables.collectToList(embeddableMappingConfiguration.getPropertiesMapping(), embeddableLinkage -> new Linkage() {
					@Override
					public ReversibleAccessor getAccessor() {
						return embeddableLinkage.getAccessor();
					}
					
					@Nullable
					@Override
					public Field getField() {
						return embeddableLinkage.getField();
					}
					
					@Nullable
					@Override
					public String getColumnName() {
						return embeddableLinkage.getColumnName();
					}
					
					@Override
					public Class getColumnType() {
						return embeddableLinkage.getColumnType();
					}
					
					@Nullable
					@Override
					public String getExtraTableName() {
						return embeddableLinkage.getExtraTableName();
					}
					
					@Override
					public ParameterBinder getParameterBinder() {
						return embeddableLinkage.getParameterBinder();
					}
					
					@Override
					public boolean isNullable() {
						return embeddableLinkage.isNullable();
					}
					
					@Override
					public boolean isReadonly() {
						return embeddableLinkage.isReadonly();
					}
				});
				
				@Override
				public Class<C> getBeanType() {
					return embeddableMappingConfiguration.getBeanType();
				}
				
				@Override
				@Nullable
				public BeanMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
					return org.codefilarete.tool.Nullable.nullable(embeddableMappingConfiguration.getMappedSuperClassConfiguration())
							.map(BeanMappingConfiguration::fromEmbeddableMappingConfiguration)
							.get();
				}
				
				@Override
				public List<Linkage> getPropertiesMapping() {
					return linkages;
				}
				
				@Override
				public Collection<Inset<C, Object>> getInsets() {
					return Iterables.collectToList(embeddableMappingConfiguration.getInsets(), embeddableInset -> new Inset<C, Object>() {
						@Override
						public Accessor<C, Object> getAccessor() {
							return embeddableInset.getAccessor();
						}
						
						@Override
						public Method getInsetAccessor() {
							return embeddableInset.getInsetAccessor();
						}
						
						@Override
						public Class<Object> getEmbeddedClass() {
							return embeddableInset.getEmbeddedClass();
						}
						
						@Override
						public ValueAccessPointSet<C> getExcludedProperties() {
							return embeddableInset.getExcludedProperties();
						}
						
						@Override
						public ValueAccessPointMap<C, String> getOverriddenColumnNames() {
							return embeddableInset.getOverriddenColumnNames();
						}
						
						@Override
						public ValueAccessPointMap<C, Column> getOverriddenColumns() {
							return embeddableInset.getOverriddenColumns();
						}
						
						@Override
						public BeanMappingConfiguration<Object> getConfiguration() {
							return fromEmbeddableMappingConfiguration(embeddableInset.getConfigurationProvider().getConfiguration());
						}
					});
				}
			};
		}
		
		
		Class<C> getBeanType();
		
		@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
		@Nullable
		BeanMappingConfiguration<? super C> getMappedSuperClassConfiguration();
		
		List<Linkage> getPropertiesMapping();
		
		Collection<Inset<C, Object>> getInsets();
		
		/**
		 * @return an iterable for all inheritance configurations, including this
		 */
		default Iterable<BeanMappingConfiguration> inheritanceIterable() {
			
			return () -> new ReadOnlyIterator<BeanMappingConfiguration>() {
				
				private BeanMappingConfiguration next = BeanMappingConfiguration.this;
				
				@Override
				public boolean hasNext() {
					return next != null;
				}
				
				@Override
				public BeanMappingConfiguration next() {
					if (!hasNext()) {
						// comply with next() method contract
						throw new NoSuchElementException();
					}
					BeanMappingConfiguration result = this.next;
					this.next = this.next.getMappedSuperClassConfiguration();
					return result;
				}
			};
		}
		
		interface Inset<SRC, TRGT> {
			
			/**
			 * Equivalent of {@link #getInsetAccessor()} as a {@link PropertyAccessor}
			 */
			Accessor<SRC, TRGT> getAccessor();
			
			/**
			 * Equivalent of given getter or setter at construction time as a {@link Method}
			 */
			Method getInsetAccessor();
			
			Class<TRGT> getEmbeddedClass();
			
			ValueAccessPointSet<SRC> getExcludedProperties();
			
			ValueAccessPointMap<SRC, String> getOverriddenColumnNames();
			
			ValueAccessPointMap<SRC, Column> getOverriddenColumns();
			
			BeanMappingConfiguration<TRGT> getConfiguration();
		}
		
		/**
		 * Small contract for defining property configuration storage
		 *
		 * @param <C> property owner type
		 * @param <O> property type
		 */
		interface Linkage<C, O> {
			
			ReversibleAccessor<C, O> getAccessor();
			
			@Nullable
			Field getField();
			
			@Nullable
			String getColumnName();
			
			Class<O> getColumnType();
			
			@Nullable
			String getExtraTableName();
			
			ParameterBinder<O> getParameterBinder();
			
			boolean isNullable();
			
			boolean isReadonly();
		}
	}
}
