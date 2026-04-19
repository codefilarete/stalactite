package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorChainMutator;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointComparator;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.PropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.ReadOnlyPropertyMapping;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingConfiguration.fromEmbeddableMappingConfiguration;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * Engine that converts mapping definition of a {@link EmbeddableMappingConfiguration} to a {@link Map} of {@link AbstractPropertyMapping}s.
 * It collects:
 * - direct properties (owned by the {@link EmbeddableMappingConfiguration})
 * - the embedded properties of the embedded configuration
 * - the properties found on inheritance by the mapped superclass definition (until no more mapped superclass is found)
 * - inherited properties of embedded configuration
 * 
 * Whereas it consumes an {@link EmbeddableMappingConfiguration} it doesn't mean that its goal is to manage embedded beans of an entity: as its
 * name says, it's aimed at collecting mapping of any beans, without the entity part (understanding identification and inheritance which is
 * {@link AggregateMetadataResolver}'s work).
 *
 * @author Guillaume Mary
 * @see #resolve(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration, Table, ColumnNamingStrategy)
 */
public class PropertyMappingResolver<C, T extends Table<T>> {
	
	private final ColumnBinderRegistry columnBinderRegistry;
	
	public PropertyMappingResolver(ColumnBinderRegistry columnBinderRegistry) {
		this.columnBinderRegistry = columnBinderRegistry;
	}
	
	/**
	 * Converts mapping definition of a {@link EmbeddableMappingConfiguration} into a simple {@link Map}
	 *
	 * @return a bean that stores some {@link Map}s representing the definition of the mapping declared by the {@link EmbeddableMappingConfiguration}
	 */
	public Set<AbstractPropertyMapping<C, ?, T>> resolve(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration<C> mainMappingConfiguration,
	                                                     T targetTable,
	                                                     ColumnNamingStrategy columnNamingStrategy) {
		return resolve(fromEmbeddableMappingConfiguration(mainMappingConfiguration),
				targetTable,
				columnNamingStrategy);
	}
	
	public Set<AbstractPropertyMapping<C, ?, T>> resolve(EmbeddableMappingConfiguration<C> mainMappingConfiguration,
	                                                     T targetTable,
	                                                     ColumnNamingStrategy columnNamingStrategy) {
		Set<AbstractPropertyMapping<C, ?, T>> result = new KeepOrderSet<>();
		
		InternalProcessor<C> internalProcessor = new InternalProcessor<>(targetTable, columnNamingStrategy);
		// converting direct mapping
		ValueAccessPointMap<C, String, ValueAccessPoint<C>> overriddenColumnNames = new ValueAccessPointMap<>();
		ValueAccessPointMap<C, Size, ValueAccessPoint<C>> overriddenColumnSizes = new ValueAccessPointMap<>();
		ValueAccessPointMap<C, Column<T, ?>, ValueAccessPoint<C>> overriddenColumns = new ValueAccessPointMap<>();
		ValueAccessPointSet<C, ?> excludedProperties = new ValueAccessPointSet<>();
		internalProcessor.includeMapping(mainMappingConfiguration,
				overriddenColumnNames,
				overriddenColumnSizes,
				overriddenColumns,
				excludedProperties);
		// adding insets
		result.addAll(includeInsets(targetTable, columnNamingStrategy, mainMappingConfiguration));
		
		// importing embeddable mapped superclass mapping
		EmbeddableMappingConfiguration<C> mappedSuperClassConfiguration = (EmbeddableMappingConfiguration<C>) mainMappingConfiguration.getMappedSuperClassConfiguration();
		while (mappedSuperClassConfiguration != null) {
			internalProcessor.includeMapping(mappedSuperClassConfiguration,
					overriddenColumnNames, overriddenColumnSizes, overriddenColumns, excludedProperties);
			// adding insets
			result.addAll(includeInsets(targetTable, columnNamingStrategy, mappedSuperClassConfiguration));
			mappedSuperClassConfiguration = (EmbeddableMappingConfiguration<C>) mappedSuperClassConfiguration.getMappedSuperClassConfiguration();
		}
		
		result.addAll(internalProcessor.result);
		
		return result;
	}
	
	private <E> Set<AbstractPropertyMapping<C, ?, T>> includeInsets(T targetTable, ColumnNamingStrategy columnNamingStrategy, EmbeddableMappingConfiguration<C> mainMappingConfiguration) {
		Set<AbstractPropertyMapping<C, ?, T>> result = new KeepOrderSet<>();
		mainMappingConfiguration.<E>getInsets().forEach(inset -> {
			InternalProcessor<E> internalProcessorForEmbeddedBeans = new InternalProcessor<>(targetTable, columnNamingStrategy);
			internalProcessorForEmbeddedBeans.includeMapping(inset.getConfiguration(),
					inset.getOverriddenColumnNames(),
					inset.getOverriddenColumnSizes(),
					(ValueAccessPointMap<E, Column<T, ?>, ValueAccessPoint<E>>) (ValueAccessPointMap) inset.getOverriddenColumns(),
					inset.getExcludedProperties());
			// shifting the embedded configuration by the inset accessor
			internalProcessorForEmbeddedBeans.result.forEach(mappingPawn -> {
				ReadWritePropertyAccessPoint<C, E> prefix = inset.getAccessor();
				AbstractPropertyMapping<C, ?, T> propertyMapping;
				if (mappingPawn instanceof ReadOnlyPropertyMapping) {
					propertyMapping = shiftMapping(prefix, (ReadOnlyPropertyMapping) mappingPawn);
				} else {
					propertyMapping = shiftMapping(prefix, (PropertyMapping) mappingPawn);
				}
				result.add(propertyMapping);
			});
		});
		return result;
	}
	
	private <X, Y> ReadOnlyPropertyMapping<C, Y, T> shiftMapping(ReadWritePropertyAccessPoint<C, X> prefix, ReadOnlyPropertyMapping<X, Y, T> mapping) {
		AccessorChainMutator<C, X, Y> shiftedAccessor = new AccessorChainMutator<>(Arrays.asList(prefix), mapping.getAccessPoint());
		return new ReadOnlyPropertyMapping<>(new ReadWriteAccessorChain<>(shiftedAccessor), mapping.getColumn(), mapping.isSetByConstructor(), mapping.getReadConverter(), mapping.isUnique());
	}
	
	private <X, Y> PropertyMapping<C, Y, T> shiftMapping(ReadWritePropertyAccessPoint<C, X> prefix, PropertyMapping<X, Y, T> mapping) {
		ReadWriteAccessorChain<C, X, Y> shiftedAccessor = new ReadWriteAccessorChain<>(Arrays.asList(prefix), mapping.getAccessPoint());
		return new PropertyMapping<>(shiftedAccessor, mapping.getColumn(), mapping.isSetByConstructor(), mapping.getReadConverter(), mapping.getWriteConverter(), mapping.isUnique());
	}
	
	/**
	 * Internal engine driven by {@link #resolve(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration, Table, ColumnNamingStrategy)} method.
	 * Made to store the result in another class than the main one and to decouple configuration from the processing.
	 *
	 * @author Guillaume Mary
	 */
	protected class InternalProcessor<E> {
		
		private final T targetTable;
		private final ColumnNamingStrategy columnNamingStrategy;
		
		private final Set<AbstractPropertyMapping<E, ?, T>> result = new KeepOrderSet<>();
		
		protected InternalProcessor(T targetTable, ColumnNamingStrategy columnNamingStrategy) {
			this.targetTable = targetTable;
			this.columnNamingStrategy = columnNamingStrategy;
		}
		
		protected <O> void includeMapping(EmbeddableMappingConfiguration<E> mappingConfiguration,
		                                  ValueAccessPointMap<E, String, ValueAccessPoint<E>> overriddenColumnNames,
		                                  ValueAccessPointMap<E, Size, ValueAccessPoint<E>> overriddenColumnSizes,
		                                  ValueAccessPointMap<E, Column<T, ?>, ValueAccessPoint<E>> overriddenColumns,
		                                  ValueAccessPointSet<E, ?> excludedProperties) {
			
			Stream<EmbeddableLinkage<E, O>> linkageStream = mappingConfiguration.<O>getPropertiesMapping().stream()
					.filter(linkage -> !excludedProperties.contains(linkage.getAccessor()));
			
			linkageStream.forEach(linkage -> {
				Column<T, O> overriddenColumn = (Column<T, O>) overriddenColumns.get(linkage.getAccessor());
				String columnName = nullable(overriddenColumn)
						.map(Column::getName)
						.getOr(() -> determineColumnName(linkage, overriddenColumnNames.get(linkage.getAccessor())));
				Size columnSize = nullable(overriddenColumn)
						.map(Column::getSize)
						.getOr(() -> determineColumnSize(linkage, overriddenColumnSizes.get(linkage.getAccessor())));
				assertMappingIsNotAlreadyDefinedByInheritance(linkage, columnName, mappingConfiguration);
				AbstractPropertyMapping<E, O, T> propertyMapping = createMapping(
						linkage,
						columnName,
						columnSize,
						overriddenColumn
				);
				result.add(propertyMapping);
			});
		}
		
		protected <O> String determineColumnName(EmbeddableLinkage<E, O> linkage, @Nullable String overriddenColumName) {
			return nullable(overriddenColumName)
					.elseSet(linkage::getColumnName)
					.elseSet(linkage::getFieldName)
					.elseSet(() -> columnNamingStrategy.giveName(AccessorDefinition.giveDefinition(linkage.getAccessor())))
					.get();
		}
		
		protected <O> Size determineColumnSize(EmbeddableLinkage<E, O> linkage, @Nullable Size overriddenColumSize) {
			return nullable(overriddenColumSize).elseSet(linkage::getColumnSize).get();
		}
		
		/**
		 * Includes a mapping of a single property.
		 * @param linkage the user-defined mapping of a property
		 * @param columnName the column name to use for this property
		 * @param columnSize the column size to use for this property, will fallbacks to a default one if not given
		 * @param overriddenColumn the column to use for this property, if not null, then the column name and size are ignored
		 * @return a coupled accessor and column
		 * @param <O> the property type
		 */
		private <O> AbstractPropertyMapping<E, O, T> createMapping(EmbeddableLinkage<E, O> linkage,
		                                                           String columnName,
		                                                           @Nullable Size columnSize,
		                                                           @Nullable Column<T, O> overriddenColumn) {
			Column<T, O> column = nullable(overriddenColumn).getOr(() -> addColumnToTable(
					linkage,
					linkage.getExtraTable() != null ? linkage.getExtraTable() : targetTable,
					columnName,
					columnSize)
			);
			
			ensureColumnBindingInRegistry(linkage, column);
			
			AbstractPropertyMapping<E, O, T> propertyMapping;
			if (!linkage.isReadonly()) {
				propertyMapping = new PropertyMapping<>(linkage.getAccessor(), column, linkage.isSetByConstructor(), linkage.getReadConverter(), linkage.getWriteConverter(), linkage.isUnique());
			} else {
				// we use getWriter() even if getLeft() is sufficient thanks to ReadWritePropertyAccessPoint implementing PropertyMutator
				// however getWrite() is more precise by removing a encapsulation layer, which potentially reflects much more user declaration
				propertyMapping = new ReadOnlyPropertyMapping<>(linkage.getAccessor().getWriter(), column, linkage.isSetByConstructor(), linkage.getReadConverter(), linkage.isUnique());
			}
			
			return propertyMapping;
		}
		
		protected <O> void assertMappingIsNotAlreadyDefinedByInheritance(EmbeddableLinkage<E, O> linkage, String columnNameToCheck, EmbeddableMappingConfiguration<E> mappingConfiguration) {
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
		
		protected <O> Column<T, O> addColumnToTable(EmbeddableLinkage<E, O> linkage, Table targetTable, String columnName, @Nullable Size columnSize) {
			Class<O> columnType;
			if (linkage.getColumnType().isEnum()) {
				if (linkage.getEnumBindType() == null) {
					columnType = (Class<O>) Enum.class;
				} else {
					columnType = linkage.getEnumBindType() == EnumBindType.NAME
							? (Class<O>) String.class
							: (Class<O>) Integer.class;
				}
			} else {
				if (linkage.getParameterBinder() != null) {
					// when a parameter binder is defined, then the column type must be binder one
					columnType = linkage.getParameterBinder().getColumnType();
				} else {
					columnType = linkage.getColumnType();
				}
			}
			// if the user asks for nullability, then we follow his demand, else we rely on the property type: primitive ones are mandatory
			Boolean isColumnNullable =
					nullable(linkage.isNullable()).getOr(() -> Reflections.isPrimitiveType(linkage.getColumnType()) ? false : null);
			return targetTable.addColumn(columnName, columnType, columnSize, isColumnNullable);
		}
		
		protected <O> void ensureColumnBindingInRegistry(EmbeddableLinkage<E, O> linkage, Column<?, O> column) {
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
	}
	
	static class DuplicateDefinitionChecker implements Consumer<EmbeddableLinkage> {
		
		private final String columnNameToCheck;
		private final ReadWritePropertyAccessPoint propertyAccessor;
		private final ColumnNamingStrategy columnNameStrategy;
		private static final ValueAccessPointComparator VALUE_ACCESS_POINT_COMPARATOR = new ValueAccessPointComparator();
		
		DuplicateDefinitionChecker(String columnNameToCheck, ReadWritePropertyAccessPoint propertyAccessor, ColumnNamingStrategy columnNameStrategy) {
			this.columnNameToCheck = columnNameToCheck;
			this.propertyAccessor = propertyAccessor;
			this.columnNameStrategy = columnNameStrategy;
		}
		
		@Override
		public void accept(EmbeddableLinkage pawn) {
			ReadWritePropertyAccessPoint accessor = pawn.getAccessor();
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
