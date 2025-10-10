package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.idpolicy.GeneratedKeysPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.DatabaseSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.PooledHiLoSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.ColumnLinkageOptions;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.CompositeKeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.EntityFactoryProvider;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.InheritanceConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.KeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.dsl.PersisterBuilder;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PersisterRegistry.DefaultPersisterRegistry;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.AfterUpdateSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.SingleColumnIdentification;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMapping;
import org.codefilarete.stalactite.engine.configurer.FluentEntityMappingConfigurationSupport.CompositeKeyLinkageSupport;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.codefilarete.stalactite.engine.configurer.polymorphism.PolymorphismPersisterBuilder;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EntityIsManagedByPersisterAsserter;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequencePersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;

import static org.codefilarete.stalactite.engine.configurer.AbstractIdentification.CompositeKeyIdentification;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.Reflections.PRIMITIVE_DEFAULT_VALUES;
import static org.codefilarete.tool.bean.Objects.preventNull;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * @author Guillaume Mary
 */
public class PersisterBuilderImpl<C, I> implements PersisterBuilder<C, I> {
	
	private final EntityMappingConfiguration<C, I> entityMappingConfiguration;
	private final MethodReferenceCapturer methodSpy;
	private ColumnBinderRegistry columnBinderRegistry;
	private Table table;
	private final Map<EntityMappingConfiguration, Table> tableMap = new HashMap<>();
	private final NamingConfiguration namingConfiguration;
	private DefaultPersisterRegistry persisterRegistry;
	
	public PersisterBuilderImpl(EntityMappingConfigurationProvider<C, I> entityMappingConfigurationProvider) {
		this(entityMappingConfigurationProvider.getConfiguration());
	}
	
	public PersisterBuilderImpl(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		this.entityMappingConfiguration = entityMappingConfiguration;
		// to be reviewed if MethodReferenceCapturer instance must be reused from another PersisterBuilderImpl (not expected because should be stateless)
		this.methodSpy = new MethodReferenceCapturer();
		NamingConfigurationCollector namingConfigurationCollector = new NamingConfigurationCollector(entityMappingConfiguration);
		this.namingConfiguration = namingConfigurationCollector.collect();
	}
	
	PersisterBuilderImpl<C, I> setColumnBinderRegistry(ColumnBinderRegistry columnBinderRegistry) {
		this.columnBinderRegistry = columnBinderRegistry;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setTable(Table table) {
		this.table = table;
		return this;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(PersistenceContext persistenceContext) {
		persisterRegistry = new DefaultPersisterRegistry(persistenceContext.getPersisters());
		ConfiguredRelationalPersister<C, I> builtPersister = build(
				persistenceContext.getDialect(),
				OptimizedUpdatePersister.wrapWithQueryCache(persistenceContext.getConnectionConfiguration()),
				this.table);
		// making aggregate persister available for external usage 
		persistenceContext.addPersister(builtPersister);
		return builtPersister;
	}
	
	/**
	 * Method for re-entrance. Made public for project usage.
	 * 
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration 
	 * @param table persistence target table
	 * @return the built persister, never null
	 */
	public ConfiguredRelationalPersister<C, I> build(Dialect dialect,
	                                                 ConnectionConfiguration connectionConfiguration,
	                                                 @Nullable Table table) {
		boolean isInitiator = PersisterBuilderContext.CURRENT.get() == null;
		
		if (isInitiator) {
			PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(persisterRegistry));
		}
		
		try {
			// If a persister already exists for the type we return it : case of graph that declares twice / several times same mapped type 
			// WARN : this does not take mapping configuration differences into account, so if configuration is different from previous one, since
			// no check is done, then the very first persister is returned
			EntityPersister<C, Object> existingPersister = PersisterBuilderContext.CURRENT.get().getPersisterRegistry().getPersister(this.entityMappingConfiguration.getEntityType());
			if (existingPersister != null) {
				// we can cast because all persisters we registered implement the interface
				return (ConfiguredRelationalPersister<C, I>) existingPersister;
			}
			ConfiguredRelationalPersister<C, I> result = doBuild(table, dialect, connectionConfiguration, PersisterBuilderContext.CURRENT.get().getPersisterRegistry());
			
			if (isInitiator) {	
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
				PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
			}
			return result;
		} finally {
			if (isInitiator) {
				PersisterBuilderContext.CURRENT.remove();
			}
		}
	}
	
	@VisibleForTesting
	<T extends Table<T>> ConfiguredRelationalPersister<C, I> doBuild(@Nullable T table,
																	 Dialect dialect,
																	 ConnectionConfiguration connectionConfiguration,
																	 PersisterRegistry persisterRegistry) {
		init(dialect.getColumnBinderRegistry(), table);
		
		mapEntityConfigurationToTable();
		
		AbstractIdentification<C, I> identification = determineIdentification();
		
		// collecting mapping from inheritance
		MappingPerTable<C> inheritanceMappingPerTable = collectPropertiesMappingFromInheritance();
		// add primary key and foreign key to all tables
		PrimaryKey<T, I> primaryKey = addIdentifyingPrimarykey(identification);
		Set<Table> inheritanceTables = inheritanceMappingPerTable.giveTables();
		inheritanceTables.remove(primaryKey.getTable());	// not necessary thanks to addColumn & addPrimaryKey tolerance to duplicates, left for clarity
		propagatePrimaryKey(primaryKey, inheritanceTables);
		List<Table> tables = new ArrayList<>(inheritanceTables);
		Collections.reverse(tables);
		applyForeignKeys(primaryKey, new KeepOrderSet<>(tables) );
		addIdentificationToMapping(identification, inheritanceMappingPerTable.getMappings());
		// determining insertion manager must be done AFTER primary key addition, else it would fall into NullPointerException
		determineIdentifierManager(identification, inheritanceMappingPerTable, identification.getIdAccessor(), dialect, connectionConfiguration);
		
		// Creating main persister 
		Mapping<C, T> mainMapping = (Mapping<C, T>) first(inheritanceMappingPerTable.getMappings());
		SimpleRelationalEntityPersister<C, I, T> mainPersister = buildMainPersister(identification, mainMapping, dialect, connectionConfiguration);
		
		applyExtraTableConfigurations(identification, mainPersister, dialect, connectionConfiguration);
		
		PersisterBuilderContext persisterBuilderContext = PersisterBuilderContext.CURRENT.get();
		RelationConfigurer<C, I> relationConfigurer = new RelationConfigurer<>(dialect, connectionConfiguration, mainPersister,
				namingConfiguration, persisterBuilderContext);
		
		persisterBuilderContext.runInContext(entityMappingConfiguration, () -> {
			// registering relations on parent entities
			// WARN : this MUST BE DONE BEFORE POLYMORPHISM HANDLING because it needs them to create adhoc joins on sub entities tables 
			inheritanceMappingPerTable.getMappings().stream()
					.map(Mapping::getMappingConfiguration)
					.filter(EntityMappingConfiguration.class::isInstance)
					.map(EntityMappingConfiguration.class::cast)
					.forEach(relationConfigurer::configureRelations);
		});
		
		ConfiguredRelationalPersister<C, I> result = mainPersister;
		// polymorphism handling
		PolymorphismPolicy<C> polymorphismPolicy = this.entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismPersisterBuilder<C, I, T> polymorphismPersisterBuilder = new PolymorphismPersisterBuilder<>(
					polymorphismPolicy, identification, mainPersister, this.columnBinderRegistry,
					mainMapping.getMapping(), mainMapping.getReadonlyMapping(),
					mainMapping.getReadConverters(),
					mainMapping.getWriteConverters(),
					this.namingConfiguration);
			result = polymorphismPersisterBuilder.build(dialect, connectionConfiguration);
		}
		
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		if (identification instanceof AbstractIdentification.SingleColumnIdentification && ((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy() instanceof AlreadyAssignedIdentifierPolicy) {
			Consumer<C> asPersistedMarker = ((AlreadyAssignedIdentifierPolicy<C, I>) ((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy()).getMarkAsPersistedFunction();
			result.addSelectListener(new SelectListener<C, I>() {
				@Override
				public void afterSelect(Set<? extends C> result) {
					Iterables.filter(result, Objects::nonNull).forEach(asPersistedMarker);
				}
			});
		}
		
		// parent persister must be kept in ascending order for further treatments
		ReadOnlyIterator<Mapping<C, ?>> inheritedMappingIterator = Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().getDelegate());
		Iterator<Mapping<C, ?>> mappings = Iterables.filter(inheritedMappingIterator,
				m -> !mainMapping.equals(m) && !m.isMappedSuperClass());
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> parentPersisters = this.buildParentPersisters(() -> mappings,
				identification, mainPersister, dialect, connectionConfiguration
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister, parentPersisters);
		
		handleVersioningStrategy(mainPersister);
		
		// we wrap final result with some transversal features
		// NB: Order of wrap is important due to invocation of instance methods with code like "this.doSomething(..)" in particular with OptimizedUpdatePersister
		// which internally calls update(C, C, boolean) on update(id, Consumer): the latter method is not listened by EntityIsManagedByPersisterAsserter
		// (because it has no purpose since entity is not given as argument) but update(C, C, boolean) is and should be, that is not the case if
		// EntityIsManagedByPersisterAsserter is done first since OptimizedUpdatePersister invokes itself with "this.update(C, C, boolean)"
		OptimizedUpdatePersister<C, I> optimizedPersister = new OptimizedUpdatePersister<>(
				new EntityIsManagedByPersisterAsserter<>(result));
		persisterRegistry.addPersister(optimizedPersister);
		parentPersisters.forEach(persisterRegistry::addPersister);
		
		return optimizedPersister;
	}
	
	private <T extends Table<T>> void applyExtraTableConfigurations(AbstractIdentification<C, I> identification,
																	SimpleRelationalEntityPersister<C, I, T> mainPersister,
																	Dialect dialect,
																	ConnectionConfiguration connectionConfiguration) {
		Map<String, Set<Linkage>> extraTableLinkages = new HashMap<>();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration1 -> {
			EntityMappingConfiguration<C, I> mappingConfiguration = (EntityMappingConfiguration<C, I>) entityMappingConfiguration1;
			List<Linkage> propertiesMapping = mappingConfiguration.getPropertiesMapping().getPropertiesMapping();
			for (Linkage linkage : propertiesMapping) {
				if (linkage.getExtraTableName() != null) {
					extraTableLinkages.computeIfAbsent(linkage.getExtraTableName(), extraTableName -> new HashSet<>()).add(linkage);
				}
			}
		});
		
		if (!extraTableLinkages.isEmpty()) {
			ExtraTableConfigurer<C, I, T> extraTableConfigurer = new ExtraTableConfigurer<>(identification, mainPersister, extraTableLinkages, columnBinderRegistry, namingConfiguration);
			extraTableConfigurer.configure(dialect, connectionConfiguration);
		}
	}
	
	
	/**
	 * Contract triggered after a persister has been built, made to complete some more configuration.
	 */
	public interface BuildLifeCycleListener {
		
		/**
		 * Invoked after main entity persister creation. At this stage all persisters involved in the entity graph
		 * are expected to be available in the {@link PersisterRegistry}
		 */
		void afterBuild();
		
		/**
		 * Invoked after all {@link #afterBuild()} of all the registered {@link BuildLifeCycleListener}s have been called.
		 * Made for finalization cases.
		 */
		void afterAllBuild();
	}
	
	/**
	 * Dedicated {@link BuildLifeCycleListener} that will consume the persister found in the registry.
	 * Used in particular to deal with bean cycle load.
	 * 
	 * @param <P> entity type to be persisted
	 * @see #consume(ConfiguredRelationalPersister)
	 */
	public static abstract class PostInitializer<P> implements BuildLifeCycleListener {
		
		/** Entity type of persister to be post initialized */
		private final Class<P> entityType;
		
		protected PostInitializer(Class<P> entityType) {
			this.entityType = entityType;
		}
		
		public Class<P> getEntityType() {
			return entityType;
		}
		
		@Override
		public final void afterBuild() {
			try {
				consume((ConfiguredRelationalPersister<P, ?>) PersisterBuilderContext.CURRENT.get().getPersisterRegistry().getPersister(entityType));
			} catch (RuntimeException e) {
				throw new MappingConfigurationException("Error while post processing persister of type "
						+ Reflections.toString(entityType), e);
			}
		}
		
		@Override
		public final void afterAllBuild() {
			// nothing special to do here
		}
		
		/**
		 * Invoked after main entity graph creation
		 * 
		 * @param persister entity type persister
		 */
		public abstract void consume(ConfiguredRelationalPersister<P, ?> persister);
	}
	
	private <T extends Table<T>> void handleVersioningStrategy(SimpleRelationalEntityPersister<C, I, T> result) {
		VersioningStrategy<C, ?> versioningStrategy = this.entityMappingConfiguration.getOptimisticLockOption();
		if (versioningStrategy != null) {
			// we have to declare it to the mapping strategy. To do that we must find the versioning column
			Column column = result.getMapping().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
			((DefaultEntityMapping) result.getMapping()).addVersionedColumn(versioningStrategy.getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versioningStrategy);
			result.getInsertExecutor().setVersioningStrategy(versioningStrategy);
		}
	}
	
	private <T extends Table<T>, TT extends Table<TT>> KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> buildParentPersisters(Iterable<Mapping<C, ?>> mappings,
																																	AbstractIdentification<C, I> identification,
																																	SimpleRelationalEntityPersister<C, I, T> mainPersister,
																																	Dialect dialect,
																																	ConnectionConfiguration connectionConfiguration) {
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> result = new KeepOrderSet<>();
		PrimaryKey<T, I> superclassPK = mainPersister.getMainTable().getPrimaryKey();
		Holder<Table> currentTable = new Holder<>(mainPersister.getMainTable());
		mappings.forEach(mapping -> {
			// we skip already configured inherited persister to avoid getting an error while adding results of buildParentPersisters(..) method
			// to the persister registry due to prohibition to add twice persister dealing with same entity type
			if (mapping.getMappingConfiguration() instanceof EmbeddableMappingConfiguration) {
				Class<?> entityType = ((EmbeddableMappingConfiguration<?>) mapping.getMappingConfiguration()).getBeanType();
				if (PersisterBuilderContext.CURRENT.get().getPersisterRegistry().getPersister(entityType) != null) {
					return;
				}
			}
			
			Mapping<C, TT> castedMapping = (Mapping<C, TT>) mapping;
			PrimaryKey<TT, I> subclassPK = castedMapping.targetTable.getPrimaryKey();
			boolean isIdentifyingConfiguration = identification.getIdentificationDefiner().getPropertiesMapping() == mapping.giveEmbeddableConfiguration();
			DefaultEntityMapping<C, I, TT> currentMappingStrategy = createEntityMapping(
					isIdentifyingConfiguration,
					castedMapping.targetTable,
					castedMapping.getMapping(),
					castedMapping.getReadonlyMapping(),
					castedMapping.getReadConverters(),
					castedMapping.getWriteConverters(),
					castedMapping.propertiesSetByConstructor,
					identification,
					mapping.giveEmbeddableConfiguration().getBeanType(),
					null);
			
			SimpleRelationalEntityPersister<C, I, TT> currentPersister = new SimpleRelationalEntityPersister<>(currentMappingStrategy, dialect, connectionConfiguration);
			result.add(currentPersister);
			// a join is necessary to select entity, only if target table changes
			if (!currentPersister.getMainTable().equals(currentTable.get())) {
				mainPersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMergerAdapter<>(currentMappingStrategy), superclassPK, subclassPK);
				currentTable.set(currentPersister.getMainTable());
			}
		});
		return result;
	}
	
	private <T extends Table<T>> SimpleRelationalEntityPersister<C, I, T> buildMainPersister(AbstractIdentification<C, I> identification,
																							 Mapping<C, T> mapping,
																							 Dialect dialect,
																							 ConnectionConfiguration connectionConfiguration) {
		EntityMappingConfiguration mappingConfiguration = (EntityMappingConfiguration) mapping.mappingConfiguration;
		// If there's some mapped inheritance, then the identifying configuration is the one with the join table
		boolean isIdentifyingConfiguration = this.entityMappingConfiguration.getInheritanceConfiguration() == null
					|| !this.entityMappingConfiguration.getInheritanceConfiguration().isJoinTable();
		DefaultEntityMapping<C, I, T> mappingStrategy = createEntityMapping(
				isIdentifyingConfiguration,
				mapping.targetTable,
				mapping.mapping,
				mapping.readonlyMapping,
				mapping.readConverters,
				mapping.writeConverters,
				mapping.propertiesSetByConstructor,
				identification,
				mappingConfiguration.getEntityType(),
				mappingConfiguration.getEntityFactoryProvider());
		return new SimpleRelationalEntityPersister<>(mappingStrategy, dialect, connectionConfiguration);
	}
	
	/**
	 * 
	 * @param mainPersister main persister
	 * @param superPersisters persisters in ascending order
	 */
	private <T extends Table<T>> void addCascadesBetweenChildAndParentTable(SimpleRelationalEntityPersister<C, I, T> mainPersister,
																			KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> superPersisters) {
		// we add cascade only on persister with different table : we keep the "lowest" one because it gets all inherited properties,
		// upper ones are superfluous
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> superPersistersWithChangingTable = new KeepOrderSet<>();
		Holder<Table> lastTable = new Holder<>(mainPersister.getMainTable());
		superPersisters.forEach(p -> {
			if (!p.getMainTable().equals(lastTable.get())) {
				superPersistersWithChangingTable.add(p);
			}
			lastTable.set(p.getMainTable());
		});
		PersisterListenerCollection<C, I> persisterListener = mainPersister.getPersisterListener();
		superPersistersWithChangingTable.forEach(superPersister -> {
			// Before insert of child we must insert parent
			persisterListener.addInsertListener(new BeforeInsertSupport<>(superPersister::insert, Function.identity()));
			
			// On child update, parent must be updated too, no constraint on order for this, after is arbitrarily chosen
			persisterListener.addUpdateListener(new AfterUpdateSupport<>(superPersister::update, Function.identity()));
			// idem for updateById
			persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
				@Override
				public void afterUpdateById(Iterable<? extends C> entities) {
					superPersister.updateById(entities);
				}
			});
			
		});
		
		List<SimpleRelationalEntityPersister<C, I, ?>> copy = Iterables.copy(superPersistersWithChangingTable);
		Collections.reverse(copy);
		copy.forEach(superPersister -> {
			// On child deletion, parent must be deleted after
			persisterListener.addDeleteListener(new AfterDeleteSupport<>(superPersister::delete, Function.identity()));
			// idem for deleteById
			persisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(superPersister::deleteById, Function.identity()));
		});
	}
	
	/**
	 * Feeds {@link this#tableMap} with inherited {@link EntityMappingConfiguration} to their {@link Table}.
	 * A table may be reused in case of inheritance configuration presence.
	 * Tables are took from given configurations, or created by using the {@link TableNamingStrategy}
	 */
	void mapEntityConfigurationToTable() {
		visitInheritedEntityMappingConfigurations(new Consumer<EntityMappingConfiguration>() {
			
			private Table currentTable = PersisterBuilderImpl.this.table;
			
			@Override
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				InheritanceConfiguration inheritanceConfiguration = entityMappingConfiguration.getInheritanceConfiguration();
				boolean changeTable = nullable(inheritanceConfiguration)
						.map(InheritanceConfiguration::isJoinTable).getOr(false);
				tableMap.put(entityMappingConfiguration, currentTable);
				if (changeTable) {
					currentTable = nullable(inheritanceConfiguration.getTable())
							.getOr(() -> new Table(namingConfiguration.getTableNamingStrategy().giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
				}
			}
		});
	}
	
	/**
	 * Sets some attributes according to given arguments and entity configuration.
	 *
	 * @param columnBinderRegistry registry used to declare columns binding
	 * @param table optional target table of the main mapping
	 * @param <T> table type
	 */
	@VisibleForTesting
	<T extends Table> void init(ColumnBinderRegistry columnBinderRegistry, @Nullable T table) {
		this.columnBinderRegistry = columnBinderRegistry;
		
		this.table = nullable(table)
				.elseSet((T) this.entityMappingConfiguration.getTable())
				.getOr(() -> (T) new Table(namingConfiguration.getTableNamingStrategy().giveName(this.entityMappingConfiguration.getEntityType())));
	}
	
	public static <C, I> void addIdentificationToMapping(AbstractIdentification<C, I> identification, Iterable<Mapping<C, ?>> mappings) {
		mappings.forEach(mapping -> mapping.registerIdentifier(identification.getIdAccessor()));
	}
	
	/**
	 * Creates primary keys on given tables with name and type of given primary key.
	 * Needs {@link this#table} to be defined.
	 *
	 * @param tables target tables on which primary keys must be added
	 * @param primaryKey
	 */
	public static void propagatePrimaryKey(PrimaryKey<?, ?> primaryKey, Set<Table> tables) {
		Holder<PrimaryKey<?, ?>> previousPk = new Holder<>(primaryKey);
		tables.forEach(t -> {
			previousPk.get().getColumns().forEach(pkColumn -> {
				Column newColumn = t.addColumn(pkColumn.getName(), pkColumn.getJavaType());
				newColumn.setNullable(false);	// may not be necessary because of primary key, let for principle
				newColumn.primaryKey();
			});
			previousPk.set(t.getPrimaryKey());
		});
	}
	
	public static void applyForeignKeys(PrimaryKey primaryKey, ForeignKeyNamingStrategy foreignKeyNamingStrategy, Set<Table> tables) {
		Holder<PrimaryKey> previousPk = new Holder<>(primaryKey);
		tables.forEach(t -> {
			PrimaryKey currentPrimaryKey = t.getPrimaryKey();
			t.addForeignKey(foreignKeyNamingStrategy.giveName(currentPrimaryKey, previousPk.get()), currentPrimaryKey, previousPk.get());
			previousPk.set(currentPrimaryKey);
		});
	}
	
	
	/**
	 * Creates primary key on table owning identification
	 * 
	 * @param identification information that allow to create primary key
	 * @return the created {@link PrimaryKey}
	 */
	@VisibleForTesting
	<T extends Table<T>> PrimaryKey<T, I> addIdentifyingPrimarykey(AbstractIdentification<C, I> identification) {
		T pkTable = (T) this.tableMap.get(identification.getIdentificationDefiner());
		KeyMapping<C, I> keyLinkage = identification.getKeyLinkage();
		AccessorDefinition identifierDefinition = AccessorDefinition.giveDefinition(identification.getIdAccessor());
		if (identification instanceof CompositeKeyIdentification) {
			CompositeKeyMappingConfiguration<I> configuration = ((CompositeKeyLinkageSupport<C, I>) keyLinkage).getCompositeKeyMappingBuilder().getConfiguration();
			// Note that we won't care about readonly column returned by build(..) since we're on a primary key case, then
			// some readonly columns would be nonsense
			BeanMappingBuilder<I, T> compositeKeyBuilder = new BeanMappingBuilder<>(configuration, pkTable, columnBinderRegistry, namingConfiguration.getColumnNamingStrategy());
			BeanMapping<I, T> build = compositeKeyBuilder.build();
			Map<ReversibleAccessor<I, Object>, Column<T, Object>> compositeKeyMapping = build.getMapping();
			compositeKeyMapping.values().forEach(Column::primaryKey);
			((CompositeKeyIdentification<C, I>) identification).setCompositeKeyMapping(compositeKeyMapping);
		} else {
			String columnName = nullable(((SingleKeyMapping<C, I>) keyLinkage).getColumnOptions()).map(ColumnLinkageOptions::getColumnName).get();
			if (columnName == null) {
				columnName = this.namingConfiguration.getColumnNamingStrategy().giveName(identifierDefinition);
			}
			Column<?, I> primaryKey = pkTable.addColumn(columnName, identifierDefinition.getMemberType());
			primaryKey.setNullable(false);	// may not be necessary because of primary key, let for principle
			primaryKey.primaryKey();
			if (((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy() instanceof GeneratedKeysPolicy) {
				primaryKey.autoGenerated();
			}
		}
		return pkTable.getPrimaryKey();
	}
	
	/**
	 * Creates foreign keys between given tables primary keys.
	 * Needs {@link this#table} to be defined.
	 *
	 * @param primaryKey initial primary key on which the very first table primary key must point to
	 * @param tables target tables on which foreign keys must be added, <strong>order matters</strong>
	 */
	@VisibleForTesting
	void applyForeignKeys(PrimaryKey primaryKey, Set<Table> tables) {
		applyForeignKeys(primaryKey, this.namingConfiguration.getForeignKeyNamingStrategy(), tables);
	}
	
	/**
	 * Gives embedded (non relational) properties mapping, including those from super classes
	 *
	 * @return the mapping between property accessor and their column in target tables, never null
	 */
	@VisibleForTesting
	<T extends Table<T>> MappingPerTable<C> collectPropertiesMappingFromInheritance() {
		MappingPerTable<C> result = new MappingPerTable<>();
		
		InheritanceMappingCollector<C, I, T> mappingCollector = new InheritanceMappingCollector<>(result, this.columnBinderRegistry, this.namingConfiguration.getColumnNamingStrategy());
		visitInheritedEmbeddableMappingConfigurations(new Consumer<EntityMappingConfiguration>() {
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
	void visitInheritedEmbeddableMappingConfigurations(Consumer<EntityMappingConfiguration> entityConfigurationConsumer,
													   Consumer<EmbeddableMappingConfiguration> mappedSuperClassConfigurationConsumer) {
		// iterating over mapping from inheritance
		Holder<EntityMappingConfiguration> lastMapping = new Holder<>();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			entityConfigurationConsumer.accept(entityMappingConfiguration);
			lastMapping.set(entityMappingConfiguration);
		});
		if (lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			// iterating over mapping from mapped super classes
			lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration().inheritanceIterable().forEach(mappedSuperClassConfigurationConsumer);
		}
	}
	
	void visitInheritedEntityMappingConfigurations(Consumer<EntityMappingConfiguration> configurationConsumer) {
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(configurationConsumer);
	}
	
	/**
	 * Looks for {@link EntityMappingConfiguration} defining the identifier policy by going up through inheritance hierarchy.
	 *
	 * @return a wrapper of necessary elements to manage entity identifier
	 * @throws UnsupportedOperationException when identification was not found, because it doesn't make sense to have an entity without identification
	 */
	private AbstractIdentification<C, I> determineIdentification() {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		if (entityMappingConfiguration.getKeyMapping() != null && entityMappingConfiguration.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier in conjunction with entity inheritance is not supported : "
					+ Reflections.toString(entityMappingConfiguration.getEntityType()) + " defines identifier "
					+ AccessorDefinition.toString(entityMappingConfiguration.getKeyMapping().getAccessor())
					+ " while it inherits from " + Reflections.toString(entityMappingConfiguration.getInheritanceConfiguration().getConfiguration().getEntityType()));
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		Holder<EntityMappingConfiguration<C, I>> configurationDefiningIdentification = new Holder<>();
		// hierarchy must be scanned to find the very first configuration that defines identification
		visitInheritedEntityMappingConfigurations(entityConfiguration -> {
			KeyMapping keyMapping = entityConfiguration.getKeyMapping();
			if (keyMapping != null &&
					(keyMapping instanceof SingleKeyMapping && ((SingleKeyMapping<?, ?>) keyMapping).getIdentifierPolicy() != null)
					|| keyMapping instanceof CompositeKeyMapping) {
				if (configurationDefiningIdentification.get() != null) {
					throw new UnsupportedOperationException("Identifier policy is defined twice in hierarchy : first by "
							+ Reflections.toString(configurationDefiningIdentification.get().getEntityType())
							+ ", then by " + Reflections.toString(entityConfiguration.getEntityType()));
				} else {
					configurationDefiningIdentification.set(entityConfiguration);
				}
			}
		});
		EntityMappingConfiguration<C, I> foundConfiguration = configurationDefiningIdentification.get();
		if (foundConfiguration == null) {
			throw newMissingIdentificationException();
		}
		
		KeyMapping<C, I> foundKeyMapping = foundConfiguration.getKeyMapping();
		if (foundKeyMapping instanceof SingleKeyMapping) {
			return AbstractIdentification.forSingleKey(foundConfiguration);
		} else if (foundKeyMapping instanceof CompositeKeyMapping) {
			assertCompositeKeyIdentifierOverridesEqualsHashcode((CompositeKeyMapping<?, ?>) foundKeyMapping);
			return AbstractIdentification.forCompositeKey(foundConfiguration, (CompositeKeyMapping<C, I>) foundKeyMapping);
		} else {
			// should not happen
			throw new MappingConfigurationException("Unknown key mapping : " + foundKeyMapping);
		}
	}
	
	@VisibleForTesting
	static void assertCompositeKeyIdentifierOverridesEqualsHashcode(CompositeKeyMapping<?, ?> compositeKeyIdentification) {
		Class<?> compositeKeyType = AccessorDefinition.giveDefinition(compositeKeyIdentification.getAccessor()).getMemberType();
		try {
			compositeKeyType.getDeclaredMethod("equals", Object.class);
			compositeKeyType.getDeclaredMethod("hashCode");
		} catch (NoSuchMethodException e) {
			throw new MappingConfigurationException("Composite key identifier class " + Reflections.toString(compositeKeyType) + " seems to have default implementation of equals() and hashcode() methods,"
					+ " which is not supported (identifiers must be distinguishable), please make it implement them");
		}
	}
	
	/**
	 * Determines {@link IdentifierInsertionManager} for current configuration as well as its whole inheritance configuration.
	 * The result is set in given {@link SingleColumnIdentification}. Could have been done on a separate object but it would have complicated some method
	 * signature, and {@link SingleColumnIdentification} is a good place for it.
	 * 
	 * @param identification given to know the expected policy and to set the result in it
	 * @param mappingPerTable necessary to get table and primary key to be read in the after-insert policy
	 * @param idAccessor id accessor to get and set identifier on entity (except for already-assigned strategy)
	 * @param dialect dialect to compute elements of identifier policy
	 * @param <E> entity type that defines identifier manager, used as internal, may be C or one of its ancestor
	 */
	private <E> void determineIdentifierManager(AbstractIdentification<E, I> identification,
												MappingPerTable<E> mappingPerTable,
												ReversibleAccessor<E, I> idAccessor,
												Dialect dialect,
												ConnectionConfiguration connectionConfiguration) {
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		Class<I> identifierType = idDefinition.getMemberType();
		IdentifierInsertionManager<E, I> identifierInsertionManager = null;
		if (identification instanceof CompositeKeyIdentification) {
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
					identifierType,
					((CompositeKeyIdentification<E, I>) identification).getMarkAsPersistedFunction(),
					((CompositeKeyIdentification<E, I>) identification).getIsPersistedFunction());
		} else {
			IdentifierPolicy<I> identifierPolicy = ((SingleColumnIdentification<E, I>) identification).getIdentifierPolicy();
			if (identifierPolicy instanceof GeneratedKeysPolicy) {
				// with identifier set by database generated key, identifier must be retrieved as soon as possible which means by the very first
				// persister, which is current one, which is the first in order of mappings
				Table<?> targetTable = first(mappingPerTable.getMappings()).targetTable;
				if (targetTable.getPrimaryKey().isComposed()) {
					throw new UnsupportedOperationException("Composite primary key is not compatible with database-generated column");
				}
				Column<?, I> primaryKey = (Column<?, I>) first(targetTable.getPrimaryKey().getColumns());
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new AccessorWrapperIdAccessor<>(idAccessor),
						dialect.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
						primaryKey.getJavaType()
				);
			} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicy) {
				Sequence<I> sequence;
				if (identifierPolicy instanceof PooledHiLoSequenceIdentifierPolicySupport) {
					Class<E> entityType = identification.getIdentificationDefiner().getEntityType();
					PooledHiLoSequenceOptions options = new PooledHiLoSequenceOptions(50, entityType.getSimpleName());
					ConnectionProvider connectionProvider = connectionConfiguration.getConnectionProvider();
					if (!(connectionProvider instanceof SeparateTransactionExecutor)) {
						throw new MappingConfigurationException("Before-insert identifier policy configured with connection that doesn't support separate transaction,"
								+ " please provide a " + Reflections.toString(SeparateTransactionExecutor.class) + " as connection provider or change identifier policy");
					}
					sequence = (Sequence<I>) new PooledHiLoSequence(options,
							new PooledHiLoSequencePersister(((PooledHiLoSequenceIdentifierPolicySupport) identifierPolicy).getStorageOptions(), dialect, (SeparateTransactionExecutor) connectionProvider, connectionConfiguration.getBatchSize()));
				} else if (identifierPolicy instanceof DatabaseSequenceIdentifierPolicySupport) {
					Class<E> entityType = identification.getIdentificationDefiner().getEntityType();
					DatabaseSequenceIdentifierPolicySupport databaseSequenceSupport = (DatabaseSequenceIdentifierPolicySupport) identifierPolicy;
					String sequenceName = databaseSequenceSupport.getDatabaseSequenceNamingStrategy().giveName(entityType);
					Database database = new Database();
					Schema sequenceSchema = nullable(databaseSequenceSupport.getDatabaseSequenceSettings().getSchemaName())
							.map(s -> database.new Schema(s))
							.elseSet(table.getSchema())
							.get();
					org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence
							= new org.codefilarete.stalactite.sql.ddl.structure.Sequence(sequenceSchema, sequenceName)
								.withBatchSize(databaseSequenceSupport.getDatabaseSequenceSettings().getBatchSize())
								.withInitialValue(databaseSequenceSupport.getDatabaseSequenceSettings().getInitialValue())
							;
					sequence = (Sequence<I>) dialect.getDatabaseSequenceSelectorFactory().create(databaseSequence, connectionConfiguration.getConnectionProvider());
				} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicySupport) {
					sequence = ((BeforeInsertIdentifierPolicySupport<I>) identifierPolicy).getSequence();
				} else {
					throw new MappingConfigurationException("Before-insert identifier policy " + Reflections.toString(identifierPolicy.getClass()) + " is not supported");
				}
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(new AccessorWrapperIdAccessor<>(idAccessor), sequence, identifierType);
			} else if (identifierPolicy instanceof AlreadyAssignedIdentifierPolicy) {
				AlreadyAssignedIdentifierPolicy<E, I> alreadyAssignedPolicy = (AlreadyAssignedIdentifierPolicy<E, I>) identifierPolicy;
				identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
						identifierType,
						alreadyAssignedPolicy.getMarkAsPersistedFunction(),
						alreadyAssignedPolicy.getIsPersistedFunction());
			}
		}
		
		// Treating configurations that are not the identifying one (for child-class) : they get an already-assigned identifier manager
		AlreadyAssignedIdentifierManager<E, I> fallbackMappingIdentifierManager = determineFallbackIdentifierManager(idAccessor, identifierInsertionManager, identifierType);
		identification
				.setInsertionManager(identifierInsertionManager)
				.setFallbackInsertionManager(fallbackMappingIdentifierManager);
	}
	
	private <E> AlreadyAssignedIdentifierManager<E, I> determineFallbackIdentifierManager(ReversibleAccessor<E, I> idAccessor,
																						  IdentifierInsertionManager<E, I> identifierInsertionManager,
																						  Class<I> identifierType) {
		AlreadyAssignedIdentifierManager<E, I> fallbackMappingIdentifierManager;
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType,
					((AlreadyAssignedIdentifierManager<E, I>) identifierInsertionManager).getMarkAsPersistedFunction(),
					((AlreadyAssignedIdentifierManager<E, I>) identifierInsertionManager).getIsPersistedFunction());
		} else {
			// auto-increment, sequence, etc : non-identifying classes get an already-assigned identifier manager based on their default value
			Function<E, Boolean> isPersistedFunction = identifierType.isPrimitive()
					? c -> PRIMITIVE_DEFAULT_VALUES.get(identifierType) == idAccessor.get(c)
					: c -> idAccessor.get(c) != null;
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType, c -> {}, isPersistedFunction);
		}
		return fallbackMappingIdentifierManager;
	}
	
	private UnsupportedOperationException newMissingIdentificationException() {
		SerializableTriFunction<FluentEntityMappingBuilder, SerializableBiConsumer<C, I>, IdentifierPolicy, FluentEntityMappingBuilderKeyOptions<C, I>>
				identifierMethodReference = FluentEntityMappingBuilder::mapKey;
		Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityMappingConfiguration.getEntityType())
				+ ", please add one through " + Reflections.toString(identifierSetter));
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
		if (identification instanceof CompositeKeyIdentification) {
			Map<ReversibleAccessor<I, Object>, Column<T, Object>> composedKeyMapping = Iterables.map(((CompositeKeyIdentification<E, I>) identification).getCompositeKeyMapping().entrySet(), Entry::getKey, entry -> targetTable.getColumn(entry.getValue().getName()));
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
		
		DefaultEntityMapping<E, I, T> result = new DefaultEntityMapping<>(beanType, targetTable, mapping, readOnlyMapping, idMappingStrategy, beanFactory, identifierSetByBeanFactory);
		result.getMainMapping().setReadConverters(readConverters);
		result.getMainMapping().setWriteConverters(writeConverters);
		propertiesSetByConstructor.forEach(result::addPropertySetByConstructor);
		return result;
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
			
			private T getTargetTable() {
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
