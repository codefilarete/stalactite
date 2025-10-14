package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.InheritanceConfiguration;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.builder.BeanMappingBuilder.BeanMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Collect properties mapping from inheritance in a form of {@link MappingPerTable}
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class InheritanceMappingStep<C, I> {
	
	/**
	 * Gives embedded (non relational) properties mapping, including those from super classes
	 *
	 * @return the mapping between property accessor and their column in target tables, never null
	 */
	@VisibleForTesting
	<T extends Table<T>> MappingPerTable<C> collectPropertiesMappingFromInheritance(EntityMappingConfiguration<C, I> entityMappingConfiguration,
																					Map<EntityMappingConfiguration, Table> tableMap,
																					ColumnBinderRegistry columnBinderRegistry,
																					ColumnNamingStrategy columnNamingStrategy) {
		MappingPerTable<C> result = new MappingPerTable<>();
		
		InheritanceMappingCollector<C, I, T> mappingCollector = new InheritanceMappingCollector<>(result, columnBinderRegistry, columnNamingStrategy);
		visitInheritedEmbeddableMappingConfigurations(entityMappingConfiguration,
				new Consumer<EntityMappingConfiguration>() {
					private boolean initMapping = false;
					
					@Override
					public void accept(EntityMappingConfiguration entityMappingConfiguration) {
						mappingCollector.currentKey = entityMappingConfiguration;
						if (initMapping) {
							mappingCollector.init();
						}
						
						mappingCollector.currentTable = (T) tableMap.get(entityMappingConfiguration);
						mappingCollector.accept(entityMappingConfiguration);
						
						// we must reinit mapping when table changes (which is a join table case), then mapping doesn't target always the same Map 
						initMapping = nullable(entityMappingConfiguration.getInheritanceConfiguration())
								.map(InheritanceConfiguration::isJoinTable).getOr(false);
					}
				}, embeddableMappingConfiguration -> {
					mappingCollector.mappedSuperClass = true;
					mappingCollector.accept(embeddableMappingConfiguration);
				});
		return result;
	}
	
	/**
	 * Visits parent {@link EntityMappingConfiguration} of current entity mapping configuration (including itself), this is an optional operation
	 * because current configuration may not have a direct entity ancestor.
	 * Then visits mapped super classes as {@link EmbeddableMappingConfiguration} of the last visited {@link EntityMappingConfiguration}, optional
	 * operation too.
	 * This is because inheritance can only have 2 paths:
	 * - first an optional inheritance from some other entity
	 * - then an optional inheritance from some mapped super class
	 *
	 * @param entityConfigurationConsumer
	 * @param mappedSuperClassConfigurationConsumer
	 */
	void visitInheritedEmbeddableMappingConfigurations(EntityMappingConfiguration<C, I> entityMappingConfiguration,
													   Consumer<EntityMappingConfiguration> entityConfigurationConsumer,
													   Consumer<EmbeddableMappingConfiguration> mappedSuperClassConfigurationConsumer) {
		// iterating over mapping from inheritance
		Holder<EntityMappingConfiguration> lastMapping = new Holder<>();
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(configuration -> {
			entityConfigurationConsumer.accept(configuration);
			lastMapping.set(configuration);
		});
		if (lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			// iterating over mapping from mapped super classes
			lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration().inheritanceIterable().forEach(mappedSuperClassConfigurationConsumer);
		}
	}
	
	public static class Mapping<C, T extends Table<T>> {
		private final Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration;
		private final T targetTable;
		private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mapping;
		private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> readonlyMapping;
		private final ValueAccessPointSet<C> propertiesSetByConstructor = new ValueAccessPointSet<>();
		private final boolean mappedSuperClass;
		private Duo<ReversibleAccessor<C, ?>, PrimaryKey<T, ?>> identifier;
		private final ValueAccessPointMap<C, Converter<Object, Object>> readConverters;
		private final ValueAccessPointMap<C, Converter<Object, Object>> writeConverters;
		
		public Mapping(Object mappingConfiguration,
					   T targetTable,
					   Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> mapping,
					   Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> readonlyMapping,
					   ValueAccessPointMap<C, ? extends Converter<Object, Object>> readConverters,
					   ValueAccessPointMap<C, ? extends Converter<Object, Object>> writeConverters,
					   boolean mappedSuperClass) {
			this.mappingConfiguration = mappingConfiguration;
			this.targetTable = targetTable;
			this.mapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mapping;
			this.readonlyMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) readonlyMapping;
			this.readConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) readConverters;
			this.writeConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) writeConverters;
			this.mappedSuperClass = mappedSuperClass;
		}
		
		public Object getMappingConfiguration() {
			return mappingConfiguration;
		}
		
		public boolean isMappedSuperClass() {
			return mappedSuperClass;
		}
		
		public EmbeddableMappingConfiguration<C> giveEmbeddableConfiguration() {
			return (EmbeddableMappingConfiguration<C>) (this.mappingConfiguration instanceof EmbeddableMappingConfiguration
					? this.mappingConfiguration
					: (this.mappingConfiguration instanceof EntityMappingConfiguration ?
					((EntityMappingConfiguration<C, T>) this.mappingConfiguration).getPropertiesMapping()
					: null));
		}
		
		public T getTargetTable() {
			return targetTable;
		}
		
		public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getMapping() {
			return mapping;
		}
		
		public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getReadonlyMapping() {
			return readonlyMapping;
		}
		
		public ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
			return readConverters;
		}
		
		public ValueAccessPointMap<C, Converter<Object, Object>> getWriteConverters() {
			return writeConverters;
		}
		
		void registerIdentifier(ReversibleAccessor<C, ?> identifierAccessor) {
			this.identifier = new Duo<>(identifierAccessor, getTargetTable().getPrimaryKey());
		}
		
		public Duo<ReversibleAccessor<C, ?>, PrimaryKey<T, ?>> getIdentifier() {
			return identifier;
		}
		
		public ValueAccessPointSet<C> getPropertiesSetByConstructor() {
			return propertiesSetByConstructor;
		}
	}
	
	public static class MappingPerTable<C> {
		
		private final KeepOrderSet<Mapping<C, ?>> mappings = new KeepOrderSet<>();
		
		<T extends Table<T>> Mapping<C, T> add(
				Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration,
				T table,
				Map<ReversibleAccessor<C, Object>, Column<T, Object>> mapping,
				Map<ReversibleAccessor<C, Object>, Column<T, Object>> readonlyMapping,
				ValueAccessPointMap<C, ? extends Converter<Object, Object>> readConverters,
				ValueAccessPointMap<C, ? extends Converter<Object, Object>> writeConverters,
				boolean mappedSuperClass) {
			Mapping<C, T> newMapping = new Mapping<>(mappingConfiguration, table,
					mapping, readonlyMapping,
					readConverters,
					writeConverters,
					mappedSuperClass);
			this.mappings.add(newMapping);
			return newMapping;
		}
		
		<T extends Table<T>> Map<ReversibleAccessor<C, Object>, Column<T, Object>> giveMapping(T table) {
			Mapping<C, T> foundMapping = (Mapping<C, T>) Iterables.find(this.mappings, m -> m.getTargetTable().equals(table));
			if (foundMapping == null) {
				throw new IllegalArgumentException("Can't find table '" + table.getAbsoluteName()
						+ "' in " + Iterables.collectToList(this.mappings, Functions.chain(Mapping::getTargetTable, Table::getAbsoluteName)).toString());
			}
			return foundMapping.mapping;
		}
		
		/**
		 * @return tables found during inheritance iteration (hence in "ascending" order)
		 */
		KeepOrderSet<Table> giveTables() {
			return Iterables.collect(this.mappings, Mapping::getTargetTable, KeepOrderSet::new);
		}
		
		public KeepOrderSet<Mapping<C, ?>> getMappings() {
			return mappings;
		}
		
	}
	
	private static class InheritanceMappingCollector<C, I, T extends Table<T>> implements Consumer<EmbeddableMappingConfiguration<C>> {
		
		private final MappingPerTable<C> result;
		
		private T currentTable;
		
		private Map<ReversibleAccessor<C, Object>, Column<T, Object>> currentColumnMap;
		
		private Map<ReversibleAccessor<C, Object>, Column<T, Object>> currentReadonlyColumnMap;
		
		private final ValueAccessPointMap<C, Converter<Object, Object>> readConverters;
		
		private final ValueAccessPointMap<C, Converter<Object, Object>> writeConverters;
		
		private Mapping<C, T> currentMapping;
		
		private Object currentKey;
		
		private boolean mappedSuperClass;
		
		private ColumnBinderRegistry columnBinderRegistry;
		private ColumnNamingStrategy columnNamingStrategy;
		
		InheritanceMappingCollector(MappingPerTable<C> result, ColumnBinderRegistry columnBinderRegistry, ColumnNamingStrategy columnNamingStrategy) {
			this.result = result;
			this.currentColumnMap = new HashMap<>();
			this.currentReadonlyColumnMap = new HashMap<>();
			this.readConverters = new ValueAccessPointMap<>();
			this.writeConverters = new ValueAccessPointMap<>();
			this.mappedSuperClass = false;
			this.columnBinderRegistry = columnBinderRegistry;
			this.columnNamingStrategy = columnNamingStrategy;
		}
		
		public void init() {
			// we can't clear those maps since they are given to some other objects, thus clearing them will impact other objects
			this.currentColumnMap = new HashMap<>();
			this.currentReadonlyColumnMap = new HashMap<>();
			this.readConverters.clear();
			this.writeConverters.clear();
			this.currentMapping = null;
		}
		
		public void accept(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
			accept(entityMappingConfiguration.getPropertiesMapping());
		}
		
		@Override
		public void accept(EmbeddableMappingConfiguration<C> embeddableMappingConfiguration) {
			BeanMappingBuilder<C, T> beanMappingBuilder = new BeanMappingBuilder<>(embeddableMappingConfiguration,
					this.currentTable,
					this.columnBinderRegistry,
					this.columnNamingStrategy);
			BeanMapping<C, T> propertiesMapping = beanMappingBuilder.build();
			ValueAccessPointSet<C> localMapping = new ValueAccessPointSet<>(currentColumnMap.keySet());
			propertiesMapping.getMapping().keySet().forEach(propertyAccessor -> {
				if (localMapping.contains(propertyAccessor)) {
					throw new MappingConfigurationException(AccessorDefinition.toString(propertyAccessor) + " is mapped twice");
				}
			});
			propertiesMapping.getReadonlyMapping().keySet().forEach(propertyAccessor -> {
				if (localMapping.contains(propertyAccessor)) {
					throw new MappingConfigurationException(AccessorDefinition.toString(propertyAccessor) + " is mapped twice");
				}
			});
			currentColumnMap.putAll(propertiesMapping.getMapping());
			currentReadonlyColumnMap.putAll(propertiesMapping.getReadonlyMapping());
			readConverters.putAll(propertiesMapping.getReadConverters());
			writeConverters.putAll(propertiesMapping.getWriteConverters());
			if (currentMapping == null) {
				currentMapping = result.add(preventNull(currentKey, embeddableMappingConfiguration), currentTable,
						// Note that we clone maps because ours are reused while iterating
						currentColumnMap, currentReadonlyColumnMap,
						new ValueAccessPointMap<>(readConverters),
						new ValueAccessPointMap<>(writeConverters),
						mappedSuperClass);
			} else {
				currentMapping = result.add(embeddableMappingConfiguration, currentTable,
						// Note that we clone maps because ours are reused while iterating
						currentColumnMap, currentReadonlyColumnMap,
						new ValueAccessPointMap<>(readConverters),
						new ValueAccessPointMap<>(writeConverters),
						mappedSuperClass);
				currentMapping.getMapping().putAll(currentColumnMap);
			}
			embeddableMappingConfiguration.getPropertiesMapping().stream()
					.filter(Linkage::isSetByConstructor).map(Linkage::getAccessor).forEach(currentMapping.propertiesSetByConstructor::add);
		}
	}
}
