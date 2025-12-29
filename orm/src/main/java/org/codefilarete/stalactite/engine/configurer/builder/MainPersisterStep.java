package org.codefilarete.stalactite.engine.configurer.builder;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.EntityFactoryProvider;
import org.codefilarete.stalactite.dsl.entity.OptimisticLockOption;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.CompositeKeyIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.SingleColumnIdentification;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.runtime.AbstractVersioningStrategy.VersioningStrategySupport;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.function.Converter;

import static org.codefilarete.tool.collection.Iterables.first;

public class MainPersisterStep<C, I> {
	
	/**
	 *
	 * @param isIdentifyingConfiguration true for a root mapping (will use {@link SingleColumnIdentification#getInsertionManager()}), false for inheritance case (will use {@link SingleColumnIdentification#getIdentificationDefiner()})
	 * @param targetTable {@link Table} to use by created {@link DefaultEntityMapping}
	 * @param mapping properties to be managed by created {@link DefaultEntityMapping}
	 * @param propertiesSetByConstructor properties set by constructor ;), to avoid re-setting them (and even look for a setter for them)
	 * @param identification {@link SingleColumnIdentification} to use (see isIdentifyingConfiguration)
	 * @param beanType entity type to be managed by created {@link DefaultEntityMapping}
	 * @param entityFactoryProvider optional, if null default bean type constructor will be used
	 * @param <E> entity type
	 * @param <I> identifier type
	 * @param <T> {@link Table} type
	 * @return a new {@link DefaultEntityMapping} built from all arguments
	 */
	public static <E, I, T extends Table<T>> DefaultEntityMapping<E, I, T> createEntityMapping(
			boolean isIdentifyingConfiguration,
			T targetTable,
			Map<? extends ReversibleAccessor<E, Object>, ? extends Column<T, Object>> mapping,
			Map<? extends ReversibleAccessor<E, Object>, ? extends Column<T, Object>> readOnlyMapping,
			ValueAccessPointMap<E, ? extends Converter<Object, Object>> readConverters,
			ValueAccessPointMap<E, ? extends Converter<Object, Object>> writeConverters,
			ValueAccessPointSet<E> propertiesSetByConstructor,
			AbstractIdentification<E, I> identification,
			Class<E> beanType,
			@Nullable EntityFactoryProvider<E, T> entityFactoryProvider) {
		return createEntityMapping(
				isIdentifyingConfiguration,
				targetTable,
				mapping,
				readOnlyMapping,
				null,	// no versioning
				readConverters,
				writeConverters,
				propertiesSetByConstructor,
				identification,
				beanType,
				entityFactoryProvider);
	}
	
	/**
	 *
	 * @param isIdentifyingConfiguration true for a root mapping (will use {@link SingleColumnIdentification#getInsertionManager()}), false for inheritance case (will use {@link SingleColumnIdentification#getIdentificationDefiner()})
	 * @param targetTable {@link Table} to use by created {@link DefaultEntityMapping}
	 * @param mapping properties to be managed by created {@link DefaultEntityMapping}
	 * @param propertiesSetByConstructor properties set by constructor ;), to avoid re-setting them (and even look for a setter for them)
	 * @param identification {@link SingleColumnIdentification} to use (see isIdentifyingConfiguration)
	 * @param beanType entity type to be managed by created {@link DefaultEntityMapping}
	 * @param entityFactoryProvider optional, if null default bean type constructor will be used
	 * @param <E> entity type
	 * @param <I> identifier type
	 * @param <T> {@link Table} type
	 * @return a new {@link DefaultEntityMapping} built from all arguments
	 */
	public static <E, I, T extends Table<T>> DefaultEntityMapping<E, I, T> createEntityMapping(
			boolean isIdentifyingConfiguration,
			T targetTable,
			Map<? extends ReversibleAccessor<E, Object>, ? extends Column<T, Object>> mapping,
			Map<? extends ReversibleAccessor<E, Object>, ? extends Column<T, Object>> readOnlyMapping,
			@Nullable Duo<? extends ReversibleAccessor<E, Object>, ? extends Column<T, Object>> versioningMapping,
			ValueAccessPointMap<E, ? extends Converter<Object, Object>> readConverters,
			ValueAccessPointMap<E, ? extends Converter<Object, Object>> writeConverters,
			ValueAccessPointSet<E> propertiesSetByConstructor,
			AbstractIdentification<E, I> identification,
			Class<E> beanType,
			@Nullable EntityFactoryProvider<E, T> entityFactoryProvider) {
		
		PrimaryKey<T, I> primaryKey = targetTable.getPrimaryKey();
		ReversibleAccessor<E, I> idAccessor = identification.getIdAccessor();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<E, I> identifierInsertionManager = isIdentifyingConfiguration
				? identification.getInsertionManager()
				: identification.getFallbackInsertionManager();
		IdMapping<E, I> idMappingStrategy;
		if (identification instanceof AbstractIdentification.CompositeKeyIdentification) {
			Map<ReversibleAccessor<I, Object>, Column<Table, Object>> compositeKeyMapping = ((CompositeKeyIdentification<E, I>) identification).getCompositeKeyMapping();
			Map<ReversibleAccessor<I, Object>, Column<T, Object>> composedKeyMapping = Iterables.map(compositeKeyMapping.entrySet(), Entry::getKey, entry -> targetTable.getColumn(entry.getValue().getName()), KeepOrderMap::new);
			ComposedIdentifierAssembler<I, T> composedIdentifierAssembler = new DefaultComposedIdentifierAssembler<>(
					targetTable,
					(Class<I>) idDefinition.getMemberType(),
					composedKeyMapping
			);
			idMappingStrategy = new ComposedIdMapping<>(idAccessor, (AlreadyAssignedIdentifierManager<E, I>) identifierInsertionManager, composedIdentifierAssembler);
		} else {
			idMappingStrategy = new SimpleIdMapping<>(idAccessor, identifierInsertionManager,
					new SingleIdentifierAssembler<>(first(primaryKey.getColumns())));
		}
		
		Function<ColumnedRow, E> beanFactory;
		boolean identifierSetByBeanFactory = true;
		if (entityFactoryProvider == null) {
			Constructor<E> constructorById = Reflections.findConstructor(beanType, idDefinition.getMemberType());
			if (constructorById == null) {
				// No constructor taking identifier as argument, we try with default constructor (no-arg one), and if
				// not available, throws an exception since we can't find a compatible constructor : user should define one
				Constructor<E> defaultConstructor = Reflections.findConstructor(beanType);
				if (defaultConstructor == null) {
					// we'll lately throw an exception (we could do it now) but the lack of constructor may be due to an abstract class in inheritance
					// path which currently won't be instanced at runtime (because its concrete subclass will be) so there's no reason to throw
					// the exception now
					beanFactory = columnValueProvider -> {
						throw new MappingConfigurationException("Entity class " + Reflections.toString(beanType) + " doesn't have a compatible accessible constructor,"
								+ " please implement a no-arg constructor or " + Reflections.toString(idDefinition.getMemberType()) + "-arg constructor");
					};
				} else {
					beanFactory = columnValueProvider -> Reflections.newInstance(defaultConstructor);
					identifierSetByBeanFactory = false;
				}
			} else {
				// Constructor with identifier as argument found
				IdentifierAssembler<I, T> identifierAssembler = idMappingStrategy.getIdentifierAssembler();
				beanFactory = columnValueProvider -> Reflections.newInstance(constructorById, identifierAssembler.assemble(columnValueProvider));
			}
		} else {
			beanFactory = entityFactoryProvider.giveEntityFactory(targetTable);
			identifierSetByBeanFactory = entityFactoryProvider.isIdentifierSetByFactory();
		}
		
		DefaultEntityMapping<E, I, T> result = new DefaultEntityMapping<>(beanType, targetTable, mapping, readOnlyMapping, versioningMapping, idMappingStrategy, beanFactory, identifierSetByBeanFactory);
		result.getMainMapping().setReadConverters(readConverters);
		result.getMainMapping().setWriteConverters(writeConverters);
		propertiesSetByConstructor.forEach(result::addPropertySetByConstructor);
		return result;
	}
	
	<T extends Table<T>> SimpleRelationalEntityPersister<C, I, T> buildMainPersister(EntityMappingConfiguration<C, I> entityMappingConfiguration,
																					 AbstractIdentification<C, I> identification,
																					 MappingPerTable<C> inheritanceMappingPerTable,
																					 NamingConfiguration namingConfiguration,
																					 Dialect dialect,
																					 ConnectionConfiguration connectionConfiguration) {
		Mapping<C, T> mainMapping = (Mapping<C, T>) first(inheritanceMappingPerTable.getMappings());
		SimpleRelationalEntityPersister<C, I, T> mainPersister = buildMainPersister(entityMappingConfiguration, identification, mainMapping, dialect, connectionConfiguration);
		
		applyExtraTableConfigurations(entityMappingConfiguration, identification, mainPersister, namingConfiguration, dialect, connectionConfiguration);
		return mainPersister;
	}
	
	private <T extends Table<T>, V> SimpleRelationalEntityPersister<C, I, T> buildMainPersister(EntityMappingConfiguration<C, I> entityMappingConfiguration,
																								AbstractIdentification<C, I> identification,
																								Mapping<C, T> mapping,
																								Dialect dialect,
																								ConnectionConfiguration connectionConfiguration) {
		EntityMappingConfiguration mappingConfiguration = (EntityMappingConfiguration) mapping.getMappingConfiguration();
		// If there's some mapped inheritance, then the identifying configuration is the one with the join table
		boolean isIdentifyingConfiguration = entityMappingConfiguration.getInheritanceConfiguration() == null
				|| !entityMappingConfiguration.getInheritanceConfiguration().isJoinTable();
		
		
		OptimisticLockOption<C, V> optimisticLockOption = (OptimisticLockOption<C, V>) entityMappingConfiguration.getOptimisticLockOption();
		DefaultEntityMapping<C, I, T> mappingStrategy = createEntityMapping(
				isIdentifyingConfiguration,
				mapping.getTargetTable(),
				mapping.getMapping(),
				mapping.getReadonlyMapping(),
				mapping.getVersioningMapping(),
				mapping.getReadConverters(),
				mapping.getWriteConverters(),
				mapping.getPropertiesSetByConstructor(),
				identification,
				mappingConfiguration.getEntityType(),
				mappingConfiguration.getEntityFactoryProvider());
		
		VersioningStrategy<C, V> versioningStrategy = null;
		if (optimisticLockOption != null) {
			versioningStrategy = new VersioningStrategySupport<>(optimisticLockOption.getVersionAccessor(), optimisticLockOption.getSerie());
		}
		return new SimpleRelationalEntityPersister<>(mappingStrategy, versioningStrategy, dialect, connectionConfiguration);
	}
	
	private <T extends Table<T>> void applyExtraTableConfigurations(EntityMappingConfiguration<C, I> entityMappingConfiguration,
																	AbstractIdentification<C, I> identification,
																	SimpleRelationalEntityPersister<C, I, T> mainPersister,
																	NamingConfiguration namingConfiguration,
																	Dialect dialect,
																	ConnectionConfiguration connectionConfiguration) {
		Map<String, Set<Linkage>> extraTableLinkages = new HashMap<>();
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(entityMappingConfiguration1 -> {
			EntityMappingConfiguration<C, I> mappingConfiguration = (EntityMappingConfiguration<C, I>) entityMappingConfiguration1;
			List<Linkage> propertiesMapping = mappingConfiguration.getPropertiesMapping().getPropertiesMapping();
			for (Linkage linkage : propertiesMapping) {
				if (linkage.getExtraTableName() != null) {
					extraTableLinkages.computeIfAbsent(linkage.getExtraTableName(), extraTableName -> new HashSet<>()).add(linkage);
				}
			}
		});
		
		if (!extraTableLinkages.isEmpty()) {
			ExtraTableConfigurer<C, I, T> extraTableConfigurer = new ExtraTableConfigurer<>(identification, mainPersister, extraTableLinkages, dialect.getColumnBinderRegistry(), namingConfiguration);
			extraTableConfigurer.configure(dialect, connectionConfiguration);
		}
	}
}
