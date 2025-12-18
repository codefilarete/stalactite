package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointComparator;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.reflection.MethodReferences.toMethodReferenceString;
import static org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration.EmbeddableLinkageSupport;
import static org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration.fromCompositeKeyMappingConfiguration;
import static org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration.fromEmbeddableMappingConfiguration;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * Engine that converts mapping definition of a {@link EmbeddableMappingConfiguration} to a simple {@link Map}.
 * Designed as such as its instances can be reused (no constructor, attributes are set through
 * {@link #build()}
 * therefore they are not thread-safe. This was made to avoid extra instance creation because this class is used in some loops.
 *
 * Whereas it consumes an {@link EmbeddableMappingConfiguration} it doesn't mean that its goal is to manage embedded beans of an entity : as its
 * name says it's aimed at collecting mapping of any beans, without the entity part (understanding identification and inheritance which is targeted
 * by {@link org.codefilarete.stalactite.engine.configurer.builder.DefaultPersisterBuilder})
 *
 * @author Guillaume Mary
 * @see #build()
 */
public class EmbeddableMappingBuilder<C, T extends Table<T>> {
	
	/**
	 * Iterates over configuration to look for any property defining a {@link Column} to use, its table would be the one to be used by builder.
	 * Throws an exception if several tables are found during iteration.
	 *
	 * @param mappingConfiguration the configuration to look up for any overriding {@link Column}
	 * @return null if no {@link Table} was found (meaning that builder is free to create one)
	 */
	public static Table giveTargetTable(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration<?> mappingConfiguration) {
		Holder<Table> result = new Holder<>();
		
		// algorithm close to the one of includeEmbeddedMapping(..)
		Queue<Inset> stack = new ArrayDeque<>(fromEmbeddableMappingConfiguration(mappingConfiguration).getInsets());
		while (!stack.isEmpty()) {
			Inset<?, ?> inset = stack.poll();
			inset.getOverriddenColumns().forEach((valueAccessPoint, targetColumn) ->
				assertHolderIsFilledWithTargetTable(result, targetColumn, valueAccessPoint)
			);
			org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration<?> configuration = inset.getConfiguration();
			stack.addAll(configuration.getInsets());
		}
		return result.get();
	}

	public static Table giveTargetTable(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration<?> mappingConfiguration, Table mainTable) {
		Holder<Table> result = new Holder<>(mainTable);

		// algorithm close to the one of includeEmbeddedMapping(..)
		Queue<Inset> stack = new ArrayDeque<>(fromEmbeddableMappingConfiguration(mappingConfiguration).getInsets());
		while (!stack.isEmpty()) {
			Inset<?, ?> inset = stack.poll();
			inset.getOverriddenColumns().forEach((valueAccessPoint, targetColumn) ->
					assertHolderIsFilledWithTargetTable(result, targetColumn, valueAccessPoint)
			);
			org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration<?> configuration = inset.getConfiguration();
			stack.addAll(configuration.getInsets());
		}
		return result.get();
	}
	
	private static void assertHolderIsFilledWithTargetTable(Holder<Table> result, Column targetColumn, ValueAccessPoint<?> valueAccessPoint) {
		if (targetColumn != null) {
			if (result.get() != null && result.get() != targetColumn.getTable()) {
				throw new MappingConfigurationException("Property " + valueAccessPoint + " overrides column with " + targetColumn.getAbsoluteName() + " but it is not part of main table " + result.get().getAbsoluteName());
			}
			result.set(targetColumn.getTable());
		}
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
		return AccessorChain.fromAccessorsWithNullSafe(
				finalAccessors,
				// this can look superfluous but fills the gap of instantiating right bean when configuration is a subtype of inset accessor,
				// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
				// (where T is type returned by accessor, or expected as input of mutator)
				(accessor, aClass) -> Reflections.newInstance(beanType));
	}
	
	private final EmbeddableMappingConfiguration<C> mainMappingConfiguration;
	private final T targetTable;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final IndexNamingStrategy indexNamingStrategy;
	
	public EmbeddableMappingBuilder(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration<C> mappingConfiguration,
									T targetTable,
									ColumnBinderRegistry columnBinderRegistry,
									ColumnNamingStrategy columnNamingStrategy,
									IndexNamingStrategy indexNamingStrategy) {
		this(fromEmbeddableMappingConfiguration(mappingConfiguration),
				targetTable,
				columnBinderRegistry,
				columnNamingStrategy,
				indexNamingStrategy);
	}
	
	public EmbeddableMappingBuilder(CompositeKeyMappingConfiguration<C> mappingConfiguration,
									T targetTable,
									ColumnBinderRegistry columnBinderRegistry,
									ColumnNamingStrategy columnNamingStrategy,
									IndexNamingStrategy indexNamingStrategy) {
		this(fromCompositeKeyMappingConfiguration(mappingConfiguration),
				targetTable,
				columnBinderRegistry,
				columnNamingStrategy,
				indexNamingStrategy);
	}
	
	@VisibleForTesting
	EmbeddableMappingBuilder(EmbeddableMappingConfiguration<C> mappingConfiguration,
							 T targetTable,
							 ColumnBinderRegistry columnBinderRegistry,
							 ColumnNamingStrategy columnNameStrategy,
							 IndexNamingStrategy indexNamingStrategy) {
		this.mainMappingConfiguration = mappingConfiguration;
		this.targetTable = targetTable;
		this.columnNamingStrategy = columnNameStrategy;
		this.columnBinderRegistry = columnBinderRegistry;
		this.indexNamingStrategy = indexNamingStrategy;
	}
	
	/**
	 * Converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a bean that stores some {@link Map}s representing the definition of the mapping declared by the {@link EmbeddableMappingConfiguration}
	 */
	public EmbeddableMapping<C, T> build() {
		return build(false);
	}
	
	public EmbeddableMapping<C, T> build(boolean onlyExtraTableLinkages) {
		return build(onlyExtraTableLinkages, new ValueAccessPointSet<>());
	}
	
	private EmbeddableMapping<C, T> build(boolean onlyExtraTableLinkages, ValueAccessPointSet<C> excludedProperties) {
		InternalProcessor internalProcessor = new InternalProcessor(onlyExtraTableLinkages);
		// converting direct mapping
		internalProcessor.includeDirectMapping(this.mainMappingConfiguration,
				null,
				new ValueAccessPointMap<>(),
				new ValueAccessPointMap<>(),
				new ValueAccessPointMap<>(),
				excludedProperties);
		// adding embeddable (no particular thought about order compared to previous direct mapping)
		internalProcessor.includeEmbeddedMapping();
		return internalProcessor.result;
	}
	
	protected T getTargetTable() {
		return targetTable;
	}
	
	protected <O> String determineColumnName(EmbeddableLinkage<C, O> linkage, @Nullable String overriddenColumName) {
		return nullable(overriddenColumName).getOr(
				() -> nullable(linkage.getColumnName())
						.elseSet(nullable(linkage.getField()).map(Field::getName))
						.getOr(() -> columnNamingStrategy.giveName(AccessorDefinition.giveDefinition(linkage.getAccessor()))));
	}
	
	protected <O> Size determineColumnSize(EmbeddableLinkage<C, O> linkage, @Nullable Size overriddenColumSize) {
		return nullable(overriddenColumSize).elseSet(linkage.getColumnSize()).get();
	}
	
	/**
	 * Internal engine driven by {@link #build()} method.
	 * Made to store result in another class than main one and decouple configuration from process.
	 *
	 * @author Guillaume Mary
	 */
	protected class InternalProcessor {
		
		private final EmbeddableMapping<C, T> result = new EmbeddableMapping<>();
		
		private final boolean onlyExtraTableLinkages;
		
		protected InternalProcessor(boolean onlyExtraTableLinkages) {
			this.onlyExtraTableLinkages = onlyExtraTableLinkages;
		}
		
		protected void includeDirectMapping(EmbeddableMappingConfiguration<?> mappingConfiguration,
											@Nullable ValueAccessPoint<C> accessorPrefix,
											ValueAccessPointMap<C, String> overriddenColumnNames,
											ValueAccessPointMap<C, Size> overriddenColumnSizes,
											ValueAccessPointMap<C, Column<T, ?>> overriddenColumns,
											ValueAccessPointSet<C> excludedProperties) {
			Stream<EmbeddableLinkage> linkageStream = mappingConfiguration.getPropertiesMapping().stream()
					.filter(linkage -> !excludedProperties.contains(linkage.getAccessor()));
			
			if (!onlyExtraTableLinkages) {
				// this method (and class) doesn't deal with extra table
				linkageStream = linkageStream.filter(linkage -> linkage.getExtraTableName() == null);
			}
			linkageStream.forEach(linkage -> {
				Column<T, ?> overriddenColumn = overriddenColumns.get(linkage.getAccessor());
				String columnName = nullable(overriddenColumn)
						.map(Column::getName)
						.getOr(() -> determineColumnName(linkage, overriddenColumnNames.get(linkage.getAccessor())));
				Size columnSize = nullable(overriddenColumn)
						.map(Column::getSize)
						.getOr(() -> determineColumnSize(linkage, overriddenColumnSizes.get(linkage.getAccessor())));
				assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
				Duo<ReversibleAccessor<C, Object>, Column<T, Object>> mapping = includeMapping(
						linkage,
						accessorPrefix,
						columnName,
						columnSize,
						overriddenColumn,
						mappingConfiguration);
				Converter<Object, Object> readConverter = linkage.getReadConverter();
				result.getReadConverters().put(mapping.getLeft(), readConverter);
				if (!linkage.isReadonly()) {
					result.getMapping().put(mapping.getLeft(), mapping.getRight());
					Converter<Object, Object> writeConverter = linkage.getWriteConverter();
					result.getWriteConverters().put(mapping.getLeft(), writeConverter);
				} else {
					result.getReadonlyMapping().put(mapping.getLeft(), mapping.getRight());
				}
			});
		}
		
		protected <O> Duo<ReversibleAccessor<C, ?>, Column<T, Object>> includeMapping(EmbeddableLinkage<C, O> linkage,
																					  @Nullable ValueAccessPoint<C> accessorPrefix,
																					  String columnName,
																					  @Nullable Size columnSize,
																					  @Nullable Column<T, O> overriddenColumn,
																					  EmbeddableMappingConfiguration<?> beanMappingConfiguration) {
			Column<T, O> column = nullable(overriddenColumn).getOr(() -> addColumnToTable(linkage, columnName, columnSize));
			
			if (linkage.isUnique() && linkage instanceof EmbeddableLinkageSupport) {
				targetTable.addIndex(
								Objects.preventNull(beanMappingConfiguration.getIndexNamingStrategy(), indexNamingStrategy).giveName(((EmbeddableLinkageSupport) linkage).getDslLinkage()),
								column)
						.setUnique();
			}
			
			ensureColumnBindingInRegistry(linkage, column);
			ReversibleAccessor<C, ?> accessor;
			if (accessorPrefix != null) {
				accessor = newAccessorChain(Collections.singletonList(accessorPrefix), linkage.getAccessor(), beanMappingConfiguration.getBeanType());
			} else {
				accessor = linkage.getAccessor();
			}
			return new Duo<>(accessor, (Column<T, Object>) column);
		}
		
		protected <O> void assertMappingIsNotAlreadyDefinedByInheritance(EmbeddableLinkage<C, O> linkage, String columnNameToCheck, EmbeddableMappingConfiguration<O> mappingConfiguration) {
			DuplicateDefinitionChecker duplicateDefinitionChecker = new DuplicateDefinitionChecker(columnNameToCheck, linkage.getAccessor(), columnNamingStrategy);
			stream(mappingConfiguration.inheritanceIterable())
					.flatMap(configuration -> (Stream<EmbeddableLinkage>) configuration.getPropertiesMapping().stream())
					// not using equals() is voluntary since we want reference checking here to exclude same instance,
					// since given linkage is one of given mappingConfiguration
					// (doing as such also prevent equals() method override to break this algorithm)
					.filter(pawn -> linkage != pawn
							// only writable properties are concerned by this check : we allow duplicates for readonly properties
							&& !pawn.isReadonly() && !linkage.isReadonly())
					.forEach(duplicateDefinitionChecker);
		}
		
		protected <O> Column<T, O> addColumnToTable(EmbeddableLinkage<C, O> linkage, String columnName, @Nullable Size columnSize) {
			Column<T, O> addedColumn;
			Class<O> columnType;
			if (linkage.getColumnType().isEnum()) {
				if (linkage.getEnumBindType() == null) {
					columnType = (Class<O>) Integer.class;
				} else {
					switch (linkage.getEnumBindType()) {
						case NAME:
							columnType = (Class<O>) String.class;
							break;
						case ORDINAL:
							columnType = (Class<O>) Integer.class;
							break;
						default:
							columnType = (Class<O>) Integer.class;
					}
				}
			} else {
				if (linkage.getParameterBinder() != null) {
					// when a parameter binder is defined, then the column type must be binder one
					columnType = linkage.getParameterBinder().getColumnType();
				} else {
					columnType = linkage.getColumnType();
				}
			}
			addedColumn = targetTable.addColumn(columnName, columnType, columnSize);
			// if user ask for nullability, then we follow his demand, else we rely on property type: primitive ones are mandatory
			boolean isColumnNullable =
					nullable(linkage.isNullable()).getOr(() -> !Reflections.isPrimitiveType(linkage.getColumnType()));
			addedColumn.setNullable(isColumnNullable);
			return addedColumn;
		}
		
		protected <O> void ensureColumnBindingInRegistry(EmbeddableLinkage<C, O> linkage, Column<?, O> column) {
			if (linkage.getColumnType().isEnum()) {
				EnumBindType enumBindType = Objects.preventNull(linkage.getEnumBindType(), EnumBindType.ORDINAL);
				columnBinderRegistry.register(column, enumBindType.newParameterBinder((Class<Enum>) linkage.getColumnType()));
			} else if (linkage.getParameterBinder() != null) {
				columnBinderRegistry.register(column, (ParameterBinder<O>) linkage.getParameterBinder());
			} else {
				try {
					// assertion to check that column binder is registered : it will throw en exception if the binder is not found
					columnBinderRegistry.getBinder(column);
				} catch (BindingException e) {
					throw new MappingConfigurationException("No binder found for property " + AccessorDefinition.toString(linkage.getAccessor())
							+ " : neither its column nor its type are registered (" + column.getAbsoluteName() + ", type " + Reflections.toString(column.getJavaType()) + ")", e);
				}
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
				
				EmbeddableMappingConfiguration<C> configuration = (EmbeddableMappingConfiguration<C>) inset.getConfiguration();
				ValueAccessPoint<C> mappingPrefix = null;
				if (inset.getAccessor() != null) {
					accessorPath.add(inset.getAccessor());
					// small optimization to avoid creation of an Accessor chain of 1 element
					if (accessorPath.size() == 1) {
						mappingPrefix = accessorPath.peek();
					} else {
						mappingPrefix = AccessorChain.fromAccessorsWithNullSafe(new ArrayList<>(accessorPath));
					}
				}
				EmbeddableMappingConfiguration<?> superClassConfiguration = configuration.getMappedSuperClassConfiguration();
				if (superClassConfiguration != null) {
					includeMappedSuperClassMapping(inset, accessorPath, superClassConfiguration);
				}
				
				includeDirectMapping(configuration,
						mappingPrefix,
						inset.getOverriddenColumnNames(),
						inset.getOverriddenColumnSizes(),
						(ValueAccessPointMap) inset.getOverriddenColumns(),
						inset.getExcludedProperties());
				if (configuration.getInsets().isEmpty()) {
					accessorPath.remove();
				} else {
					stack.addAll(configuration.getInsets());
				}
				treatedInsets.add(inset);
			}
		}
		
		private void includeMappedSuperClassMapping(Inset<C, ?> inset, Collection<Accessor<C, ?>> accessorPath, EmbeddableMappingConfiguration<?> superClassConfiguration) {
			// we include super type mapping by using a new instance of EmbeddableMappingBuilder, this is the simplest (but maybe not the most
			// debuggable) and allows to manage inheritance of several mappedSuperClass 
			ValueAccessPointSet<C> excludedProperties = new ValueAccessPointSet<>();
			// we remove overridden inset columns to avoid their creation by the EmbeddableMappingBuilder
			// to avoid duplicates because they are already in the target table (through their creation) and the builder
			// will create them with their default name
			excludedProperties.addAll(inset.getOverriddenColumns().keySet());
			EmbeddableMappingBuilder<C, T> mappedSuperClassBuilder = new EmbeddableMappingBuilder<C, T>((EmbeddableMappingConfiguration<C>) superClassConfiguration, targetTable,
					columnBinderRegistry, columnNamingStrategy, Objects.preventNull(superClassConfiguration.getIndexNamingStrategy(), indexNamingStrategy)) {
				
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<C, O> linkage, @Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, inset.getOverriddenColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<C, O> linkage, @Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, inset.getOverriddenColumnSizes().get(linkage.getAccessor()));
				}
			};
			
			EmbeddableMapping<C, T> superMapping = mappedSuperClassBuilder.build(false, excludedProperties);
			Class<?> insetBeanType = inset.getConfiguration().getBeanType();
			superMapping.getMapping().forEach((accessor, column) -> {
				AccessorChain prefix;
				List<Accessor<?, ?>> accessors;
				if (accessorPath.size() == 1) {
					accessors = Arrays.asList(Iterables.first(accessorPath), accessor);
				} else {
					accessors = new ArrayList<>(accessorPath);
					accessors.add(accessor);
				}
				prefix = AccessorChain.fromAccessorsWithNullSafe(
						accessors,
						// this can look superfluous but fills the gap with instantiating right bean when configuration is a subtype of inset accessor,
						// case which is allowed by signature of embed(..) method : it accepts "? extend T" as parameter type of given configuration
						// (where T is type returned by accessor, or expected as input of mutator)
						(localAccessor, accessorInputType) -> Reflections.newInstance(insetBeanType));
				
				// Computing definitive column because it may be overridden by inset declaration
				Column<T, ?> finalColumn;
				if (inset.getOverriddenColumns().containsKey(accessor)) {
					finalColumn = inset.getOverriddenColumns().get(accessor);
				} else if (inset.getOverriddenColumnNames().containsKey(accessor)) {
					finalColumn = targetTable.addColumn(inset.getOverriddenColumnNames().get(accessor), column.getJavaType());
				} else {
					finalColumn = targetTable.addColumn(column.getName(), column.getJavaType());
				}
				result.getMapping().put((ReversibleAccessor<C, Object>) prefix, (Column<T, Object>) finalColumn);
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
				EmbeddableMappingConfiguration<?> insetConfiguration = inset.getConfiguration();
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
	
	static class DuplicateDefinitionChecker implements Consumer<EmbeddableLinkage> {
		
		private final String columnNameToCheck;
		private final ReversibleAccessor propertyAccessor;
		private final ColumnNamingStrategy columnNameStrategy;
		private static final ValueAccessPointComparator VALUE_ACCESS_POINT_COMPARATOR = new ValueAccessPointComparator();
		
		DuplicateDefinitionChecker(String columnNameToCheck, ReversibleAccessor propertyAccessor, ColumnNamingStrategy columnNameStrategy) {
			this.columnNameToCheck = columnNameToCheck;
			this.propertyAccessor = propertyAccessor;
			this.columnNameStrategy = columnNameStrategy;
		}
		@Override
		public void accept(EmbeddableLinkage pawn) {
			ReversibleAccessor accessor = pawn.getAccessor();
			if (VALUE_ACCESS_POINT_COMPARATOR.compare(accessor, propertyAccessor) == 0) {
				throw new MappingConfigurationException("Mapping is already defined by method " + AccessorDefinition.toString(propertyAccessor));
			} else if (columnNameToCheck.equals(pawn.getColumnName())
						|| columnNameToCheck.equals(columnNameStrategy.giveName(AccessorDefinition.giveDefinition(accessor)))) {
				throw new MappingConfigurationException("Column '" + columnNameToCheck + "' of mapping '" + AccessorDefinition.toString(propertyAccessor)
						+ "' is already targeted by '" + AccessorDefinition.toString(pawn.getAccessor()) + "'");
			}
		}
	}
}
