package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnOptions.AfterInsertIdentifierPolicy;
import org.codefilarete.stalactite.engine.ColumnOptions.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.engine.ColumnOptions.BeforeInsertIdentifierPolicy;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration.ColumnLinkageOptions;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration.KeyMapping;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterBuilder;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.AfterUpdateSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
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
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.PersistentFieldHarvester;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;

import static org.codefilarete.tool.Nullable.nullable;
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
	private TableNamingStrategy tableNamingStrategy;
	private final Map<EntityMappingConfiguration, Table> tableMap = new HashMap<>();
	private ColumnNamingStrategy columnNamingStrategy;
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private JoinColumnNamingStrategy joinColumnNamingStrategy;
	private ColumnNamingStrategy indexColumnNamingStrategy;
	private AssociationTableNamingStrategy associationTableNamingStrategy;
	private ColumnNameProvider columnNameProvider;
	
	public PersisterBuilderImpl(EntityMappingConfigurationProvider<C, I> entityMappingConfigurationProvider) {
		this(entityMappingConfigurationProvider.getConfiguration());
	}
	
	public PersisterBuilderImpl(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		this.entityMappingConfiguration = entityMappingConfiguration;
		// to be reviewed if MethodReferenceCapturer instance must be reused from another PersisterBuilderImpl (not expected because should be stateless)
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	PersisterBuilderImpl<C, I> setColumnBinderRegistry(ColumnBinderRegistry columnBinderRegistry) {
		this.columnBinderRegistry = columnBinderRegistry;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setTable(Table table) {
		this.table = table;
		return this;
	}
	
	public PersisterBuilderImpl<C, I> setElementCollectionTableNamingStrategy(ElementCollectionTableNamingStrategy tableNamingStrategy) {
		this.elementCollectionTableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		this.columnNameProvider = new ColumnNameProvider(this.columnNamingStrategy) {
			/** Overridden to invoke join column naming strategy if necessary */
			@Override
			protected String giveColumnName(Linkage linkage) {
				if (PersisterBuilderContext.CURRENT.get().isEntity(linkage.getColumnType())) {
					return columnNamingStrategy.giveName(AccessorDefinition.giveDefinition(linkage.getAccessor()));
				} else {
					return super.giveColumnName(linkage);
				}
			}
		};
		return this;
	}
	
	PersisterBuilderImpl<C, I> setForeignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(PersistenceContext persistenceContext, @Nullable Table table) {
		return build(
				persistenceContext.getDialect(),
				OptimizedUpdatePersister.wrapWithQueryCache(persistenceContext.getConnectionConfiguration()),
				persistenceContext,
				table);
	}
	
	/**
	 * Method for reentrance. Made public for project usage.
	 * 
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration 
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 * @param table persistence target table
	 * @return the built persister, never null
	 */
	public ConfiguredRelationalPersister<C, I> build(Dialect dialect,
														   ConnectionConfiguration connectionConfiguration,
														   PersisterRegistry persisterRegistry,
														   @Nullable Table table) {
		boolean isInitiator = PersisterBuilderContext.CURRENT.get() == null;
		
		if (isInitiator) {
			PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext());
		}
		
		try {
			// If a persister already exists for the type we return it : case of graph that declares twice / several times same mapped type 
			// WARN : this does not take mapping configuration differences into account, so if configuration is different from previous one, since
			// no check is done, then the very first persister is returned
			EntityPersister<C, Object> existingPersister = persisterRegistry.getPersister(this.entityMappingConfiguration.getEntityType());
			if (existingPersister != null) {
				// we can cast because all persisters we registered implement the interface
				return (ConfiguredRelationalPersister<C, I>) existingPersister;
			}
			ConfiguredRelationalPersister<C, I> result = doBuild(table, dialect::buildGeneratedKeysReader,
					dialect, connectionConfiguration, persisterRegistry);
			
			if (isInitiator) {	
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				PersisterBuilderContext.CURRENT.get().getPostInitializers().forEach(invocation -> {
					try {
						invocation.consume((ConfiguredRelationalPersister) persisterRegistry.getPersister(invocation.getEntityType()));
					} catch (RuntimeException e) {
						throw new MappingConfigurationException("Error while post processing persister of type "
								+ Reflections.toString(invocation.getEntityType()), e);
					}
				});
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
																	 GeneratedKeysReaderBuilder generatedKeysReaderBuilder,
																	 Dialect dialect,
																	 ConnectionConfiguration connectionConfiguration,
																	 PersisterRegistry persisterRegistry) {
		init(dialect.getColumnBinderRegistry(), table);
		
		mapEntityConfigurationPerTable();
		
		Identification<C, I> identification = determineIdentification();
		
		// collecting mapping from inheritance
		MappingPerTable<C> inheritanceMappingPerTable = collectPropertiesMappingFromInheritance();
		// add primary key and foreign key to all tables
		PrimaryKey<T, I> primaryKey = (PrimaryKey<T, I>) addIdentifyingPrimarykey(identification, inheritanceMappingPerTable);
		Set<Table> inheritanceTables = inheritanceMappingPerTable.giveTables();
		inheritanceTables.remove(primaryKey.getTable());	// not necessary thanks to addColumn & addPrimaryKey tolerance to duplicates, left for clarity
		propagatePrimaryKey(primaryKey, inheritanceTables);
		List<Table> tables = new ArrayList<>(inheritanceTables);
		Collections.reverse(tables);
		applyForeignKeys(primaryKey, new KeepOrderSet<>(tables) );
		addIdentificationToMapping(identification, inheritanceMappingPerTable.getMappings());
		// determining insertion manager must be done AFTER primary key addition, else it would fall into NullPointerException
		determineIdentifierManager(identification, inheritanceMappingPerTable, identification.getIdAccessor(), generatedKeysReaderBuilder);
		
		// Creating main persister 
		Mapping<C, T> mainMapping = (Mapping<C, T>) first(inheritanceMappingPerTable.getMappings());
		SimpleRelationalEntityPersister<C, I, T> mainPersister = buildMainPersister(identification, mainMapping, dialect, connectionConfiguration);
		PersisterBuilderContext.CURRENT.get().addEntity(mainPersister.getMapping().getClassToPersist());
		
		RelationConfigurer<C, I, ?> relationConfigurer = new RelationConfigurer<>(dialect, connectionConfiguration, persisterRegistry, mainPersister,
				columnNamingStrategy,
				foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy,
				joinColumnNamingStrategy,
				indexColumnNamingStrategy,
				associationTableNamingStrategy);
		
		PersisterBuilderContext.CURRENT.get().runInContext(entityMappingConfiguration, () -> {
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
					polymorphismPolicy, identification, mainPersister, this.columnBinderRegistry, this.columnNameProvider,
					this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy,
					this.joinColumnNamingStrategy, this.indexColumnNamingStrategy,
					this.associationTableNamingStrategy, (Map) mainMapping.getMapping(), this.tableNamingStrategy);
			result = polymorphismPersisterBuilder.build(dialect, connectionConfiguration, persisterRegistry);
		}
		
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		if (entityMappingConfiguration.getKeyMapping() != null && entityMappingConfiguration.getKeyMapping().getIdentifierPolicy() instanceof AlreadyAssignedIdentifierPolicy) {
			Consumer<C> asPersistedMarker = ((AlreadyAssignedIdentifierPolicy<C, I>) entityMappingConfiguration.getKeyMapping().getIdentifierPolicy()).getMarkAsPersistedFunction();
			result.addSelectListener(new SelectListener<C, I>() {
				@Override
				public void afterSelect(Iterable<? extends C> result) {
					Iterables.filter(result, Objects::nonNull).forEach(asPersistedMarker);
				}
			});
		}
		
		// parent persister must be kept in ascending order for further treatments
		ReadOnlyIterator<Mapping<C, ?>> mappingReadOnlyIterator = Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().getSurrogate());
		Iterator<Mapping<C, ?>> mappings = Iterables.filter(mappingReadOnlyIterator,
				m -> !mainMapping.equals(m) && !m.isMappedSuperClass());
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> parentPersisters = this.buildParentPersisters(() -> mappings,
				identification, mainPersister, dialect, connectionConfiguration
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister, parentPersisters);
		
		handleVersioningStrategy(mainPersister);
		
		// we wrap final result with some transversal features
		// NB: Order of wrap is important due to invocation of instance methods with code like "this.doSomething(..)" in particular with OptimizedUpdatePersister
		// which internaly calls update(C, C, boolean) on update(id, Consumer): the latter method is not listened by EntityIsManagedByPersisterAsserter
		// (because it has no purpose since entity is not given as argument) but update(C, C, boolean) is and should be, that is not the case if
		// EntityIsManagedByPersisterAsserter is done first since OptimizedUpdatePersister invokes itself with "this.update(C, C, boolean)"
		OptimizedUpdatePersister<C, I> optimizedPersister = new OptimizedUpdatePersister<>(
				new EntityIsManagedByPersisterAsserter<>(result));
		persisterRegistry.addPersister(optimizedPersister);
		parentPersisters.forEach(persisterRegistry::addPersister);
		
		return optimizedPersister;
	}
	
	/**
	 * Contract triggered after a persister has been built, made to fulfill some more configuration.
	 * Used in particular to deal with bean cycle load.
	 * 
	 * @param <P> entity type to be persisted
	 * @see #consume(ConfiguredRelationalPersister) 
	 */
	public static abstract class PostInitializer<P> {
		
		/** Entity type of persister to be post initialized */
		private final Class<P> entityType;
		
		protected PostInitializer(Class<P> entityType) {
			this.entityType = entityType;
		}
		
		public Class<P> getEntityType() {
			return entityType;
		}
		
		/**
		 * Invoked after main entity graph creation
		 * 
		 * @param persister entity type persister
		 */
		public abstract void consume(ConfiguredRelationalPersister<P, ?> persister);
	}
	
	public interface BuildLifeCycleListener {
		
		/**
		 * Invoked after main entity graph creation
		 */
		void afterAllBuild();
	}
	
	private <T extends Table<T>> void handleVersioningStrategy(SimpleRelationalEntityPersister<C, I, T> result) {
		VersioningStrategy<C, ?> versioningStrategy = this.entityMappingConfiguration.getOptimisticLockOption();
		if (versioningStrategy != null) {
			// we have to declare it to the mapping strategy. To do that we must find the versioning column
			Column column = result.getMapping().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
			((ClassMapping) result.getMapping()).addVersionedColumn(versioningStrategy.getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versioningStrategy);
			result.getInsertExecutor().setVersioningStrategy(versioningStrategy);
		}
	}
	
	private <T extends Table<T>, TT extends Table<TT>> KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> buildParentPersisters(Iterable<Mapping<C, ?>> mappings,
																																	Identification<C, I> identification,
																																	SimpleRelationalEntityPersister<C, I, T> mainPersister,
																																	Dialect dialect,
																																	ConnectionConfiguration connectionConfiguration) {
		KeepOrderSet<SimpleRelationalEntityPersister<C, I, ?>> parentPersisters = new KeepOrderSet<>();
		PrimaryKey<T, I> superclassPK = mainPersister.getMainTable().getPrimaryKey();
		Holder<Table> currentTable = new Holder<>(mainPersister.getMainTable());
		mappings.forEach(mapping -> {
			Mapping<C, TT> castedMapping = (Mapping<C, TT>) mapping;
			PrimaryKey<TT, I> subclassPK = castedMapping.targetTable.getPrimaryKey();
			ClassMapping<C, I, TT> currentMappingStrategy = this.createClassMappingStrategy(
					identification.getIdentificationDefiner().getPropertiesMapping() == mapping.giveEmbeddableConfiguration(),
					castedMapping.targetTable,
					castedMapping.getMapping(),
					castedMapping.propertiesSetByConstructor,
					identification,
					mapping.giveEmbeddableConfiguration().getBeanType(),
					null);
			
			SimpleRelationalEntityPersister<C, I, TT> currentPersister = new SimpleRelationalEntityPersister<>(currentMappingStrategy, dialect, connectionConfiguration);
			parentPersisters.add(currentPersister);
			// a join is necessary to select entity, only if target table changes
			if (!currentPersister.getMainTable().equals(currentTable.get())) {
				mainPersister.getEntityMappingTreeSelectExecutor().addMergeJoin(EntityJoinTree.ROOT_STRATEGY_NAME, currentMappingStrategy,
																				superclassPK, subclassPK);
				currentTable.set(currentPersister.getMainTable());
			}
		});
		return parentPersisters;
	}
	
	private <T extends Table<T>> SimpleRelationalEntityPersister<C, I, T> buildMainPersister(Identification<C, I> identification,
																							 Mapping<C, T> mapping,
																							 Dialect dialect,
																							 ConnectionConfiguration connectionConfiguration) {
		EntityMappingConfiguration mappingConfiguration = (EntityMappingConfiguration) mapping.mappingConfiguration;
		ClassMapping<C, I, T> parentMappingStrategy = createClassMappingStrategy(
				identification.getIdentificationDefiner().getPropertiesMapping() == mappingConfiguration.getPropertiesMapping(),
				mapping.targetTable,
				mapping.mapping,
				mapping.propertiesSetByConstructor,
				identification,
				mappingConfiguration.getEntityType(),
				mappingConfiguration.getEntityFactoryProvider());
		return new SimpleRelationalEntityPersister<>(parentMappingStrategy, dialect, connectionConfiguration);
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
					superPersister.updateById((Iterable<C>) entities);
				}
			});
			
		});
		
		List<SimpleRelationalEntityPersister<C, I, ?>> copy = Iterables.copy(superPersistersWithChangingTable);
		java.util.Collections.reverse(copy);
		copy.forEach(superPersister -> {
			// On child deletion, parent must be deleted after
			persisterListener.addDeleteListener(new AfterDeleteSupport<>(superPersister::delete, Function.identity()));
			// idem for deleteById
			persisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(superPersister::deleteById, Function.identity()));
		});
	}
	
	void mapEntityConfigurationPerTable() {
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
							.getOr(() -> new Table(tableNamingStrategy.giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
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
		
		org.codefilarete.tool.Nullable<TableNamingStrategy> optionalTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getTableNamingStrategy() != null && !optionalTableNamingStrategy.isPresent()) {
				optionalTableNamingStrategy.set(configuration.getTableNamingStrategy());
			}
		});
		this.tableNamingStrategy = optionalTableNamingStrategy.getOr(TableNamingStrategy.DEFAULT);
		
		this.table = nullable(table).getOr(() -> (T) new Table(tableNamingStrategy.giveName(this.entityMappingConfiguration.getEntityType())));
		
		// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too
		org.codefilarete.tool.Nullable<ColumnNamingStrategy> optionalColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		class ColumnNamingStrategyCollector implements Consumer<EmbeddableMappingConfiguration> {
			@Override
			public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
				if (embeddableMappingConfiguration.getColumnNamingStrategy() != null && !optionalColumnNamingStrategy.isPresent()) {
					optionalColumnNamingStrategy.set(embeddableMappingConfiguration.getColumnNamingStrategy());
				}
			}
		}
		ColumnNamingStrategyCollector columnNamingStrategyCollector = new ColumnNamingStrategyCollector();
		visitInheritedEmbeddableMappingConfigurations(entityConfigurationConsumer ->
				columnNamingStrategyCollector.accept(entityConfigurationConsumer.getPropertiesMapping()), columnNamingStrategyCollector);
		setColumnNamingStrategy(optionalColumnNamingStrategy.getOr(ColumnNamingStrategy.DEFAULT));
		
		org.codefilarete.tool.Nullable<ForeignKeyNamingStrategy> optionalForeignKeyNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getForeignKeyNamingStrategy() != null && !optionalForeignKeyNamingStrategy.isPresent()) {
				optionalForeignKeyNamingStrategy.set(configuration.getForeignKeyNamingStrategy());
			}
		});
		this.foreignKeyNamingStrategy = optionalForeignKeyNamingStrategy.getOr(ForeignKeyNamingStrategy.DEFAULT);
		
		org.codefilarete.tool.Nullable<JoinColumnNamingStrategy> optionalJoinColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getJoinColumnNamingStrategy() != null && !optionalJoinColumnNamingStrategy.isPresent()) {
				optionalJoinColumnNamingStrategy.set(configuration.getJoinColumnNamingStrategy());
			}
		});
		this.joinColumnNamingStrategy = optionalJoinColumnNamingStrategy.getOr(JoinColumnNamingStrategy.JOIN_DEFAULT);
		
		org.codefilarete.tool.Nullable<ColumnNamingStrategy> optionalIndexColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getIndexColumnNamingStrategy() != null && !optionalIndexColumnNamingStrategy.isPresent()) {
				optionalIndexColumnNamingStrategy.set(configuration.getIndexColumnNamingStrategy());
			}
		});
		this.indexColumnNamingStrategy = optionalIndexColumnNamingStrategy.getOr(ColumnNamingStrategy.INDEX_DEFAULT);
		
		org.codefilarete.tool.Nullable<AssociationTableNamingStrategy> optionalAssociationTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getAssociationTableNamingStrategy() != null && !optionalAssociationTableNamingStrategy.isPresent()) {
				optionalAssociationTableNamingStrategy.set(configuration.getAssociationTableNamingStrategy());
			}
		});
		this.associationTableNamingStrategy = optionalAssociationTableNamingStrategy.getOr(AssociationTableNamingStrategy.DEFAULT);
		
		org.codefilarete.tool.Nullable<ElementCollectionTableNamingStrategy> optionalElementCollectionTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getElementCollectionTableNamingStrategy() != null && !optionalElementCollectionTableNamingStrategy.isPresent()) {
				optionalElementCollectionTableNamingStrategy.set(configuration.getElementCollectionTableNamingStrategy());
			}
		});
		this.elementCollectionTableNamingStrategy = optionalElementCollectionTableNamingStrategy.getOr(ElementCollectionTableNamingStrategy.DEFAULT);
	}
	
	public static <C, I> void addIdentificationToMapping(Identification<C, I> identification, Iterable<Mapping<C, ?>> mappings) {
		mappings.forEach(mapping -> mapping.registerIdentifier(identification.getIdAccessor()));
	}
	
	/**
	 * Creates primary keys on given tables with name and type of given primary key.
	 * Needs {@link this#table} to be defined.
	 *
	 * @param tables target tables on which primary keys must be added
	 * @param primaryKey
	 */
	public static void propagatePrimaryKey(PrimaryKey primaryKey, Set<Table> tables) {
		Column pkColumn = (Column) first(primaryKey.getColumns());
		Holder<Column> previousPk = new Holder<>(pkColumn);
		tables.forEach(t -> {
			Column newColumn = t.addColumn(pkColumn.getName(), pkColumn.getJavaType());
			newColumn.setNullable(false);	// may not be necessary because of primary key, let for principle
			newColumn.primaryKey();
			previousPk.set(newColumn);
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
	PrimaryKey<?, I> addIdentifyingPrimarykey(Identification<C, I> identification, MappingPerTable<C> inheritanceMappingPerTable) {
		Table<?> pkTable = this.tableMap.get(identification.identificationDefiner);
		KeyMapping<C, I> keyLinkage = identification.getKeyLinkage();
		AccessorDefinition identifierDefinition = AccessorDefinition.giveDefinition(identification.getIdAccessor());
		if (keyLinkage instanceof EntityMappingConfiguration.SingleKeyMapping) {
			String columnName = nullable(((SingleKeyMapping<C, I>) keyLinkage).getColumnOptions()).map(ColumnLinkageOptions::getColumnName).get();
			if (columnName == null) {
				columnName = columnNamingStrategy.giveName(identifierDefinition);
			}
			Column<?, I> primaryKey = pkTable.addColumn(columnName, identifierDefinition.getMemberType());
			primaryKey.setNullable(false);	// may not be necessary because of primary key, let for principle
			primaryKey.primaryKey();
			if (identification.getIdentifierPolicy() instanceof AfterInsertIdentifierPolicy) {
				primaryKey.autoGenerated();
			}
		} else if (keyLinkage instanceof EntityMappingConfiguration.CompositeKeyMapping) {
			PersistentFieldHarvester fieldHarverster = new PersistentFieldHarvester();
			Map<PropertyAccessor<Object, Object>, Column<Table, Object>> propertyAccessorColumnMap = fieldHarverster.<Object, Table>mapFields(identifierDefinition.getDeclaringClass(), pkTable);
			propertyAccessorColumnMap.values().forEach(column -> {
				column.setNullable(false);	// may not be necessary because of primary key, let for principle
				column.primaryKey();
			});
		} else {
			throw new UnsupportedOperationException("Key linkage " + Reflections.toString(keyLinkage.getClass()) + " is not supported");
		}
		return pkTable.getPrimaryKey();
	}
	
	/**
	 * Creates foreign keys between given tables primary keys.
	 * Needs {@link this#table} to be defined.
	 *
	 * @param primaryKey initial primary key on which the very first table primary key must points to
	 * @param tables target tables on which foreign keys must be added, <strong>order matters</strong>
	 */
	@VisibleForTesting
	void applyForeignKeys(PrimaryKey primaryKey, Set<Table> tables) {
		applyForeignKeys(primaryKey, this.foreignKeyNamingStrategy, tables);
	}
	
	/**
	 * Gives embedded (non relational) properties mapping, including those from super classes
	 *
	 * @return the mapping between property accessor and their column in target tables, never null
	 */
	@VisibleForTesting
	MappingPerTable<C> collectPropertiesMappingFromInheritance() {
		MappingPerTable<C> result = new MappingPerTable<>();
		
		class MappingCollector<T extends Table<T>> implements Consumer<EmbeddableMappingConfiguration<C>> {
			
			private final BeanMappingBuilder<C, T> beanMappingBuilder = new BeanMappingBuilder<>();
			
			private T currentTable;
			
			private Map<ReversibleAccessor<C, Object>, Column<T, Object>> currentColumnMap = new HashMap<>();
			
			private Mapping<C, T> currentMapping;
			
			private Object currentKey;
			
			private boolean mappedSuperClass = false;
			
			@Override
			public void accept(EmbeddableMappingConfiguration<C> embeddableMappingConfiguration) {
				Map<ReversibleAccessor<C, Object>, Column<T, Object>> propertiesMapping = beanMappingBuilder.build(
						embeddableMappingConfiguration,
						this.currentTable,
						PersisterBuilderImpl.this.columnBinderRegistry,
						columnNameProvider);
				ValueAccessPointSet localMapping = new ValueAccessPointSet(currentColumnMap.keySet());
				propertiesMapping.keySet().forEach(propertyAccessor -> {
					if (localMapping.contains(propertyAccessor)) {
						throw new MappingConfigurationException(AccessorDefinition.toString(propertyAccessor) + " is mapped twice");
					}
				});
				currentColumnMap.putAll(propertiesMapping);
				if (currentMapping == null) {
					currentMapping = result.add(preventNull(currentKey, embeddableMappingConfiguration), currentTable, currentColumnMap, mappedSuperClass);
				} else {
					currentMapping = result.add(embeddableMappingConfiguration, currentTable, currentColumnMap, mappedSuperClass);
					currentMapping.getMapping().putAll(currentColumnMap);
				}
				embeddableMappingConfiguration.getPropertiesMapping().stream()
						.filter(Linkage::isSetByConstructor).map(Linkage::getAccessor).forEach(currentMapping.propertiesSetByConstructor::add);
			}
			
			public void accept(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
				accept(entityMappingConfiguration.getPropertiesMapping());
			}
		}
		
		
		MappingCollector mappingCollector = new MappingCollector();
		visitInheritedEmbeddableMappingConfigurations(new Consumer<EntityMappingConfiguration>() {
			private boolean initMapping = false;
			
			@Override
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				mappingCollector.currentKey = entityMappingConfiguration;
				if (initMapping) {
					mappingCollector.currentColumnMap = new HashMap<>();
					mappingCollector.currentMapping = null;
				}
				
				mappingCollector.currentTable = tableMap.get(entityMappingConfiguration);
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
	 * This is because inheritance can only have 2 paths :
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
	private Identification<C, I> determineIdentification() {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		if (entityMappingConfiguration.getKeyMapping() != null && entityMappingConfiguration.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported : "
					+ Reflections.toString(entityMappingConfiguration.getEntityType()) + " defines identifier "
					+ AccessorDefinition.toString(entityMappingConfiguration.getKeyMapping().getAccessor())
					+ " while it inherits from " + Reflections.toString(entityMappingConfiguration.getInheritanceConfiguration().getConfiguration().getEntityType()));
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		Holder<EntityMappingConfiguration<C, I>> configurationDefiningIdentification = new Holder<>();
		// hierarchy must be scanned to find the very first configuration that defines identification
		visitInheritedEntityMappingConfigurations(entityConfiguration -> {
			if (entityConfiguration.getKeyMapping() != null && entityConfiguration.getKeyMapping().getIdentifierPolicy() != null) {
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
		return new Identification<>(foundConfiguration);
	}
	
	/**
	 * Determines {@link IdentifierInsertionManager} for current configuration as weel as its whole inheritance configuration.
	 * The result is set in given {@link Identification}. Could have been done on a separatate object but it would have complexified some method
	 * signature, and {@link Identification} is a good place for it.
	 * 
	 * @param identification given to know expected policy, and to set result in it
	 * @param mappingPerTable necessary to get table and primary key to be read in after-insert policy
	 * @param idAccessor id accessor to get and set identifier on entity (except for already-assigned strategy)
	 * @param generatedKeysReaderBuilder reader for {@link AfterInsertIdentifierPolicy}
	 * @param <E> entity type that defines identifier manager, used as internal, may be C or one of its ancestor
	 */
	private <E> void determineIdentifierManager(Identification<E, I> identification,
												MappingPerTable<E> mappingPerTable,
												ReversibleAccessor<E, I> idAccessor,
												GeneratedKeysReaderBuilder generatedKeysReaderBuilder) {
		IdentifierInsertionManager<E, I> identifierInsertionManager = null;
		IdentifierPolicy<I> identifierPolicy = identification.getIdentifierPolicy();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		Class<I> identifierType = idDefinition.getMemberType();
		if (identifierPolicy instanceof AfterInsertIdentifierPolicy) {
			// with identifier set by database generated key, identifier must be retrieved as soon as possible which means by the very first
			// persister, which is current one, which is the first in order of mappings
			Table<?> targetTable = first(mappingPerTable.getMappings()).targetTable;
			if (targetTable.getPrimaryKey().isComposed()) {
				throw new UnsupportedOperationException("Composite primary key is not compatible with database-generated column");
			}
			Column<?, I> primaryKey = (Column<?, I>) first(targetTable.getPrimaryKey().getColumns());
			identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
					new SinglePropertyIdAccessor<>(idAccessor),
					generatedKeysReaderBuilder.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
					primaryKey.getJavaType()
			);
		} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicy) {
			identifierInsertionManager = new BeforeInsertIdentifierManager<>(
					new SinglePropertyIdAccessor<>(idAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
		} else if (identifierPolicy instanceof AlreadyAssignedIdentifierPolicy) {
			AlreadyAssignedIdentifierPolicy<E, I> alreadyAssignedPolicy = (AlreadyAssignedIdentifierPolicy<E, I>) identifierPolicy;
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
					identifierType,
					alreadyAssignedPolicy.getMarkAsPersistedFunction(),
					alreadyAssignedPolicy.getIsPersistedFunction());
		}
		
		// Treating configurations that are not the identifying one (for child-class) : they get an already-assigned identifier manager
		AlreadyAssignedIdentifierManager<E, I> fallbackMappingIdentifierManager;
		if (identifierPolicy instanceof AlreadyAssignedIdentifierPolicy) {
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType,
					((AlreadyAssignedIdentifierPolicy<E, I>) identifierPolicy).getMarkAsPersistedFunction(),
					((AlreadyAssignedIdentifierPolicy<E, I>) identifierPolicy).getIsPersistedFunction());
		} else {
			Function<E, Boolean> isPersistedFunction;
			if (!identifierType.isPrimitive()) {
				isPersistedFunction = c -> idAccessor.get(c) != null;
			} else {
				isPersistedFunction = c -> Reflections.PRIMITIVE_DEFAULT_VALUES.get(identifierType) == idAccessor.get(c);
			}
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType, c -> { }, isPersistedFunction);
		}
		identification.insertionManager = identifierInsertionManager;
		identification.fallbackInsertionManager = fallbackMappingIdentifierManager;
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
	 * @param isIdentifyingConfiguration true for a root mapping (will use {@link Identification#insertionManager}), false for inheritance case (will use {@link Identification#identificationDefiner}) 
	 * @param targetTable {@link Table} to use by created {@link ClassMapping}
	 * @param mapping properties to be managed by created {@link ClassMapping}
	 * @param propertiesSetByConstructor properties set by constructor ;), to avoid re-setting them (and even look for a setter for them) 
	 * @param identification {@link Identification} to use (see isIdentifyingConfiguration)
	 * @param beanType entity type to be managed by created {@link ClassMapping}
	 * @param entityFactoryProvider optional, if null default bean type constructor will be used
	 * @param <E> entity type
	 * @param <I> identifier type
	 * @param <T> {@link Table} type
	 * @return a new {@link ClassMapping} built from all arguments
	 */
	public static <E, I, T extends Table<T>> ClassMapping<E, I, T> createClassMappingStrategy(
			boolean isIdentifyingConfiguration,
			T targetTable,
			Map<ReversibleAccessor<E, Object>, Column<T, Object>> mapping,
			ValueAccessPointSet propertiesSetByConstructor,
			Identification<E, I> identification,
			Class<E> beanType,
			@Nullable EntityMappingConfiguration.EntityFactoryProvider<E, T> entityFactoryProvider) {
		
		PrimaryKey<T, I> primaryKey = targetTable.getPrimaryKey();
		ReversibleAccessor<E, I> idAccessor = identification.getIdAccessor();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<E, I> identifierInsertionManager = isIdentifyingConfiguration
				? identification.insertionManager
				: identification.fallbackInsertionManager;
		IdMapping<E, I> idMappingStrategy;
		if (primaryKey.isComposed()) {
			Map<? extends ReversibleAccessor, Column> composedKeyMapping = null;
			Class<I> keyType = idDefinition.getMemberType();
			Constructor<I> defaultConstructor = Reflections.findConstructor(keyType);
			ComposedIdentifierAssembler<I, T> composedIdentifierAssembler = new ComposedIdentifierAssembler<I, T>(targetTable) {
				
				@Override
				public Map<Column<T, Object>, Object> getColumnValues(I id) {
					Map<Column<T, Object>, Object> result = new HashMap<>();
					composedKeyMapping.forEach((propertyAccessor, column) -> result.put(column, propertyAccessor.get(id)));
					return result;
				}
				
				@Nullable
				@Override
				public I assemble(Function<Column<?, ?>, Object> columnValueProvider) {
					// we should not return an id if any value is null
					Function<Function<Column, Object>, I> keyFactory;
					if (defaultConstructor == null) {
						// we'll lately throw an exception (we could do it now) but the lack of constructor may be due to an abstract class in inheritance
						// path which currently won't be instanced at runtime (because its concrete subclass will be) so there's no reason to throw
						// the exception now
						keyFactory = keyValueProvider -> {
							throw new MappingConfigurationException("Key class " + Reflections.toString(keyType) + " doesn't have a compatible accessible constructor,"
									+ " please implement a no-arg constructor or " + Reflections.toString(idDefinition.getMemberType()) + "-arg constructor");
						};
					} else {
						keyFactory = keyValueProvider -> Reflections.newInstance(defaultConstructor);
					}
					return keyFactory.apply(columnValueProvider::apply);
				}
			};
			idMappingStrategy = new ComposedIdMapping<>(idAccessor, identifierInsertionManager, composedIdentifierAssembler);
		} else {
			idMappingStrategy = new SimpleIdMapping<>(idAccessor, identifierInsertionManager,
					new SimpleIdentifierAssembler<>(first(primaryKey.getColumns())));
		}

		Function<Function<Column<?, ?>, Object>, E> beanFactory;
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
		
		ClassMapping<E, I, T> result = new ClassMapping<>(beanType, targetTable, mapping, idMappingStrategy, beanFactory, identifierSetByBeanFactory);
		propertiesSetByConstructor.forEach(result::addPropertySetByConstructor);
		return result;
	}
	
	public static class Identification<C, I> {
		
		private final ReversibleAccessor<C, I> idAccessor;
		private final IdentifierPolicy<I> identifierPolicy;
		private final EntityMappingConfiguration<C, I> identificationDefiner;
		private final KeyMapping<C, I> keyLinkage;
		
		/** Insertion manager for {@link ClassMapping} that owns identifier policy */
		private IdentifierInsertionManager<C, I> insertionManager;
		/** Insertion manager for {@link ClassMapping} that doesn't own identifier policy : they get an already-assigned one */
		private AlreadyAssignedIdentifierManager<C, I> fallbackInsertionManager;
		
		
		public Identification(EntityMappingConfiguration<C, I> identificationDefiner) {
			this.keyLinkage = identificationDefiner.getKeyMapping();
			this.idAccessor = identificationDefiner.getKeyMapping().getAccessor();
			this.identifierPolicy = identificationDefiner.getKeyMapping().getIdentifierPolicy();
			this.identificationDefiner = identificationDefiner;
		}
		
		public KeyMapping<C, I> getKeyLinkage() {
			return keyLinkage;
		}
		
		public ReversibleAccessor<C, I> getIdAccessor() {
			return idAccessor;
		}
		
		public IdentifierPolicy<I> getIdentifierPolicy() {
			return identifierPolicy;
		}
		
		public EntityMappingConfiguration<C, I> getIdentificationDefiner() {
			return identificationDefiner;
		}
	}
	
	public static class MappingPerTable<C> {
		
		private final KeepOrderSet<Mapping<C, ?>> mappings = new KeepOrderSet<>();
		
		<T extends Table<T>> Mapping<C, T> add(
				Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration,
				T table,
				Map<ReversibleAccessor<C, Object>, Column<T, Object>> mapping,
				boolean mappedSuperClass) {
			Mapping<C, T> newMapping = new Mapping<>(mappingConfiguration, table, mapping, mappedSuperClass);
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
			private final ValueAccessPointSet propertiesSetByConstructor = new ValueAccessPointSet();
			private final boolean mappedSuperClass;
			private Duo<ReversibleAccessor<C, ?>, PrimaryKey<T, ?>> identifier;
			
			public Mapping(Object mappingConfiguration, T targetTable, Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> mapping, boolean mappedSuperClass) {
				this.mappingConfiguration = mappingConfiguration;
				this.targetTable = targetTable;
				this.mapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mapping;
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
			
			void registerIdentifier(ReversibleAccessor<C, ?> identifierAccessor) {
				this.identifier = new Duo<>(identifierAccessor, getTargetTable().getPrimaryKey());
			}
			
			public Duo<ReversibleAccessor<C, ?>, PrimaryKey<T, ?>> getIdentifier() {
				return identifier;
			}
		}
	}
	
	@FunctionalInterface
	@VisibleForTesting
	interface GeneratedKeysReaderBuilder {
		GeneratedKeysReader buildGeneratedKeysReader(String keyName, Class columnType);
	}
}
