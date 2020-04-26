package org.gama.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.function.Functions;
import org.gama.lang.function.Hanger.Holder;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.CascadeManyConfigurer;
import org.gama.stalactite.persistence.engine.CascadeOne;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnOptions;
import org.gama.stalactite.persistence.engine.ColumnOptions.BeforeInsertIdentifierPolicy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.PersisterBuilder;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.VersioningStrategy;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.ElementCollectionLinkage;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport.EntityGraphNode;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.function.Predicates.not;
import static org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy.AFTER_INSERT;

/**
 * @author Guillaume Mary
 */
public class PersisterBuilderImpl<C, I> implements PersisterBuilder<C, I> {
	
	/**
	 * Tracker of entities that are mapped along the build process. Used for column naming of relations : columns that target an entity may use a
	 * different strategy than simple properties, in particular for reverse column naming or bidirectional relation.
	 * Made static because several {@link PersisterBuilderImpl}s are instanciated along the build process.
	 * Not the best design ever, but works !
	 */
	@VisibleForTesting
	static final ThreadLocal<Set<Class>> ENTITY_CANDIDATES = new ThreadLocal<>();
	
	private final EntityMappingConfiguration<C, I> entityMappingConfiguration;
	private final MethodReferenceCapturer methodSpy;
	private ColumnBinderRegistry columnBinderRegistry;
	private Table table;
	private TableNamingStrategy tableNamingStrategy;
	private final Map<EntityMappingConfiguration, Table> tableMap = new HashMap<>();
	private ColumnNamingStrategy columnNamingStrategy;
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private ColumnNamingStrategy joinColumnNamingStrategy;
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
			/** Overriden to invoke join column naming strategy if necessary */
			@Override
			protected String giveColumnName(Linkage linkage) {
				if (ENTITY_CANDIDATES.get().contains(linkage.getColumnType())) {
					return joinColumnNamingStrategy.giveName(AccessorDefinition.giveDefinition(linkage.getAccessor()));
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
	public IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
	}
	
	@Override
	public IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext, @Nullable Table table) {
		boolean isInitiator = ENTITY_CANDIDATES.get() == null;
		
		if (isInitiator) {
			ENTITY_CANDIDATES.set(new HashSet<>());
		}
		
		try {
			return doBuild(persistenceContext, table);
		} finally {
			if (isInitiator) {
				ENTITY_CANDIDATES.remove();
			}
		}
	}
	
	private IEntityConfiguredPersister<C, I> doBuild(PersistenceContext persistenceContext, @Nullable Table table) {
		init(persistenceContext.getDialect().getColumnBinderRegistry(), table);
		
		mapEntityConfigurationPerTable();
		
		Identification identification = determineIdentification();
		
		// collecting mapping from inheritance
		MappingPerTable inheritanceMappingPerTable = collectEmbeddedMappingFromInheritance();
		// TODO: check if this can be done just before cascade ones addition
		inheritanceMappingPerTable.getMappings().forEach(mapping -> {
			if (mapping.getMappingConfiguration() instanceof EntityMappingConfiguration) {
				((EntityMappingConfiguration<C, I>) mapping.getMappingConfiguration()).getOneToOnes().forEach(cascadeOne -> {
					if (!cascadeOne.isOwnedByReverseSide()) {
						AccessorDefinition targetProviderDefinition = AccessorDefinition.giveDefinition(cascadeOne.getTargetProvider());
						mapping.getMapping().put(
								cascadeOne.getTargetProvider(),
								mapping.getTargetTable().addColumn(joinColumnNamingStrategy.giveName(targetProviderDefinition),
										targetProviderDefinition.getMemberType()));
					}
				});
			}
		});
		
		// add primary key and foreign key to all tables
		addPrimarykeys(identification, inheritanceMappingPerTable.giveTables());
		addForeignKeys(identification, inheritanceMappingPerTable.giveTables());
		addIdentificationToMapping(identification, inheritanceMappingPerTable.getMappings());
		
		// Creating main persister 
		Mapping mainMapping = Iterables.first(inheritanceMappingPerTable.getMappings());
		JoinedTablesPersister<C, I, Table> mainPersister = buildMainPersister(identification, mainMapping, persistenceContext);
		ENTITY_CANDIDATES.get().add(mainPersister.getMappingStrategy().getClassToPersist());
		
		// registering relations on parent entities
		// WARN : this MUST BE DONE BEFORE POLYMORPHISM HANDLING because it needs them to create adhoc joins on sub entities tables 
		inheritanceMappingPerTable.getMappings().stream()
				.map(Mapping::getMappingConfiguration)
				.filter(EntityMappingConfiguration.class::isInstance)
				.map(EntityMappingConfiguration.class::cast)
				.forEach(entityMappingConfiguration -> registerRelationCascades(entityMappingConfiguration, persistenceContext, mainPersister));
		
		IEntityConfiguredPersister<C, I> result = mainPersister;
		// polymorphism handling
		PolymorphismPolicy polymorphismPolicy = this.entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismBuilder<C, I, Table> polymorphismBuilder = null;
			if (polymorphismPolicy instanceof SingleTablePolymorphism) {
				polymorphismBuilder = new SingleTablePolymorphismBuilder<>((SingleTablePolymorphism<C, I, ?>) polymorphismPolicy,
						identification, mainPersister, mainMapping, this.columnBinderRegistry, this.columnNameProvider);
			} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
				polymorphismBuilder = new TablePerClassPolymorphismBuilder<C, I, Table>((TablePerClassPolymorphism<C, I>) polymorphismPolicy,
						identification, mainPersister, mainMapping, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy) {
					@Override
					void addPrimarykey(Identification identification, Table table) {
						PersisterBuilderImpl.this.addPrimarykeys(identification, Arrays.asSet(table));
					}
					
					@Override
					void addIdentificationToMapping(Identification identification, Mapping mapping) {
						PersisterBuilderImpl.this.addIdentificationToMapping(identification, Arrays.asSet(mapping));
					}
				};
			} else if (polymorphismPolicy instanceof JoinedTablesPolymorphism) {
				polymorphismBuilder = new JoinedTablesPolymorphismBuilder<C, I, Table>((JoinedTablesPolymorphism<C, I>) polymorphismPolicy,
						identification, mainPersister, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy) {
					@Override
					void addPrimarykey(Identification identification, Table table) {
						PersisterBuilderImpl.this.addPrimarykeys(identification, Arrays.asSet(table));
					}
					
					@Override
					void addForeignKey(Identification identification, Table table) {
						PersisterBuilderImpl.this.addForeignKeys(identification, Arrays.asSet(table));
					}
					
					@Override
					void addIdentificationToMapping(Identification identification, Mapping mapping) {
						PersisterBuilderImpl.this.addIdentificationToMapping(identification, Arrays.asSet(mapping));
					}
				};
			}
			result = polymorphismBuilder.build(persistenceContext);
			// we transfert listeners by principle and in particular for StatefullIdentifier state change and relation cascade triggering
			result.addInsertListener(mainPersister.getPersisterListener().getInsertListener());
			result.addUpdateListener(mainPersister.getPersisterListener().getUpdateListener());
			result.addSelectListener(mainPersister.getPersisterListener().getSelectListener());
			result.addDeleteListener(mainPersister.getPersisterListener().getDeleteListener());
			result.addDeleteByIdListener(mainPersister.getPersisterListener().getDeleteByIdListener());
		}
		
		
		
		// parent persister must be kept in ascending order for further treatments
		Iterator<Mapping> mappings = Iterables.filter(Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().asSet()),
				m -> !mainMapping.equals(m) && !m.mappedSuperClass);
		KeepOrderSet<Persister<C, I, Table>> parentPersisters = buildParentPersisters(() -> mappings,
				identification, mainPersister, persistenceContext
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister, parentPersisters);
		
		handleVersioningStrategy(mainPersister);
		
		persistenceContext.addPersister(result);
		parentPersisters.forEach(persistenceContext::addPersister);
		
		return result;
	}
	
	private <T extends Table> void registerRelationCascades(EntityMappingConfiguration<C, I> entityMappingConfiguration,
															PersistenceContext persistenceContext,
															JoinedTablesPersister<C, I, T> sourcePersister) {
		for (CascadeOne<C, ?, ?> cascadeOne : entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			cascadeOneConfigurer.appendCascade(cascadeOne, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy);
		}
		for (CascadeMany<C, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.associationTableNamingStrategy);
		}
		registerRelationsInGraph(entityMappingConfiguration, sourcePersister.getCriteriaSupport().getRootConfiguration(), persistenceContext);
		
		// taking element collections into account
		for (ElementCollectionLinkage<C, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionCascadeConfigurer elementCollectionCascadeConfigurer = new ElementCollectionCascadeConfigurer(persistenceContext);
			elementCollectionCascadeConfigurer.appendCascade(elementCollection, sourcePersister, foreignKeyNamingStrategy, columnNamingStrategy,
					elementCollectionTableNamingStrategy);
		}
	}
	
	private <T extends Table> void handleVersioningStrategy(JoinedTablesPersister<C, I, T> result) {
		VersioningStrategy versioningStrategy = this.entityMappingConfiguration.getOptimisticLockOption();
		if (versioningStrategy != null) {
			// we have to declare it to the mapping strategy. To do that we must find the versionning column
			Column column = result.getMappingStrategy().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
			((ClassMappingStrategy) result.getMappingStrategy()).addVersionedColumn(versioningStrategy.getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versioningStrategy);
			result.getInsertExecutor().setVersioningStrategy(versioningStrategy);
		}
	}
	
	/**
	 * Adds one-to-one and one-to-many graph node to the given root. Used for select by entity properties because without this it could not load
	 * whole entity graph
	 *
	 * @param configurationSupport entity mapping configuration which relations must be registered onto target
	 * @param target the node on which to add sub graph elements
	 * @param persistenceContext used as a per-entity {@link org.gama.stalactite.persistence.mapping.IEntityMappingStrategy} registry 
	 */
	private void registerRelationsInGraph(EntityMappingConfiguration configurationSupport, EntityGraphNode target, PersistenceContext persistenceContext) {
		List<CascadeMany> oneToManys = configurationSupport.getOneToManys();
		oneToManys.forEach((CascadeMany cascadeMany) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeMany.getCollectionProvider(),
					((IConfiguredPersister) persistenceContext.getPersister(cascadeMany.getTargetMappingConfiguration().getEntityType())).getMappingStrategy());
			registerRelationsInGraph(cascadeMany.getTargetMappingConfiguration(), entityGraphNode, persistenceContext);
		});
		List<CascadeOne> oneToOnes = configurationSupport.getOneToOnes();
		oneToOnes.forEach((CascadeOne cascadeOne) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeOne.getTargetProvider(),
					((IConfiguredPersister) persistenceContext.getPersister(cascadeOne.getTargetMappingConfiguration().getEntityType())).getMappingStrategy());
			registerRelationsInGraph(cascadeOne.getTargetMappingConfiguration(), entityGraphNode, persistenceContext);
		});
	}
	
	private <T extends Table> KeepOrderSet<Persister<C, I, Table>> buildParentPersisters(Iterable<Mapping> mappings,
																						 Identification identification,
																						 JoinedTablesPersister<C, I, T> mainPersister,
																						 PersistenceContext persistenceContext) {
		KeepOrderSet<Persister<C, I, Table>> parentPersisters = new KeepOrderSet<>();
		Column superclassPK = (Column) Iterables.first(mainPersister.getMainTable().getPrimaryKey().getColumns());
		Holder<Table> currentTable = new Holder<>(mainPersister.getMainTable());
		mappings.forEach(mapping -> {
			Column subclassPK = (Column) Iterables.first(mapping.targetTable.getPrimaryKey().getColumns());
			mapping.mapping.put(identification.getIdAccessor(), subclassPK);
			ClassMappingStrategy<C, I, Table> currentMappingStrategy = createClassMappingStrategy(
					identification.getIdentificationDefiner().getPropertiesMapping() == mapping.giveEmbeddableConfiguration(),
					mapping.targetTable,
					mapping.mapping,
					mapping.propertiesSetByConstructor,
					identification,
					persistenceContext.getDialect()::buildGeneratedKeysReader,
					mapping.giveEmbeddableConfiguration().getBeanType(),
					null);
			
			JoinedTablesPersister<C, I, Table> currentPersister = new JoinedTablesPersister<>(persistenceContext, currentMappingStrategy);
			parentPersisters.add(currentPersister);
			// a join is necessary to select entity, only if target table changes
			if (!currentPersister.getMainTable().equals(currentTable.get())) {
				mainPersister.getJoinedStrategiesSelectExecutor().addComplementaryJoin(JoinedStrategiesSelect.ROOT_STRATEGY_NAME, currentMappingStrategy,
					superclassPK, subclassPK);
				currentTable.set(currentPersister.getMainTable());
			}
		});
		return parentPersisters;
	}
	
	private <T extends Table> JoinedTablesPersister<C, I, T> buildMainPersister(Identification identification, Mapping mapping, PersistenceContext persistenceContext) {
		EntityMappingConfiguration entityMappingConfiguration = (EntityMappingConfiguration) mapping.mappingConfiguration;
		ClassMappingStrategy<C, I, T> parentMappingStrategy = createClassMappingStrategy(
				identification.getIdentificationDefiner().getPropertiesMapping() == entityMappingConfiguration.getPropertiesMapping(),
				mapping.targetTable,
				mapping.mapping,
				mapping.propertiesSetByConstructor,
				identification,
				persistenceContext.getDialect()::buildGeneratedKeysReader,
				entityMappingConfiguration.getEntityType(),
				entityMappingConfiguration.getEntityFactory());
		return (JoinedTablesPersister<C, I, T>) new JoinedTablesPersister(persistenceContext, parentMappingStrategy);
	}
	
	interface PolymorphismBuilder<C, I, T extends Table> {
		
		IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext);
	}
	
	/**
	 * 
	 * @param mainPersister main persister
	 * @param superPersisters persisters in ascending order
	 */
	private void addCascadesBetweenChildAndParentTable(Persister<C, I, ? extends Table> mainPersister, KeepOrderSet<Persister<C, I, Table>> superPersisters) {
		// we add cascade only on persister with different table : we keep the "lowest" one because it gets all inherited properties,
		// upper ones are superfluous
		KeepOrderSet<Persister<C, I, Table>> superPersistersWithChangingTable = new KeepOrderSet<>();
		Holder<Table> lastTable = new Holder<>(mainPersister.getMainTable());
		superPersisters.forEach(p -> {
			if (!p.getMainTable().equals(lastTable.get())) {
				superPersistersWithChangingTable.add(p);
			}
			lastTable.set(p.getMainTable());
		});
		PersisterListener<C, I> persisterListener = mainPersister.getPersisterListener();
		superPersistersWithChangingTable.forEach(superPersister -> {
			// Before insert of child we must insert parent
			persisterListener.addInsertListener(new BeforeInsertSupport<>(superPersister::insert, Function.identity()));
			
			// On child update, parent must be updated too, no constraint on order for this, after is arbitrarly choosen
			persisterListener.addUpdateListener(new AfterUpdateSupport<>(superPersister::update, Function.identity()));
			// idem for updateById
			persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
				@Override
				public void afterUpdateById(Iterable<C> entities) {
					superPersister.updateById(entities);
				}
			});
			
		});
		
		List<Persister<C, I, Table>> copy = Iterables.copy(superPersistersWithChangingTable);
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
				Table table;
				tableMap.put(entityMappingConfiguration, currentTable);
				if (changeTable) {
					table = nullable(inheritanceConfiguration.getTable())
							.getOr(() -> new Table(tableNamingStrategy.giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
					currentTable = table;
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
		
		org.gama.lang.Nullable<TableNamingStrategy> optionalTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getTableNamingStrategy() != null && !optionalTableNamingStrategy.isPresent()) {
				optionalTableNamingStrategy.set(entityMappingConfiguration.getTableNamingStrategy());
			}
		});
		this.tableNamingStrategy = optionalTableNamingStrategy.getOr(TableNamingStrategy.DEFAULT);
		
		this.table = nullable(table).getOr(() -> (T) new Table(tableNamingStrategy.giveName(this.entityMappingConfiguration.getEntityType())));
		
		// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too
		org.gama.lang.Nullable<ColumnNamingStrategy> optionalColumnNamingStrategy = org.gama.lang.Nullable.empty();
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
		
		org.gama.lang.Nullable<ForeignKeyNamingStrategy> optionalForeignKeyNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getForeignKeyNamingStrategy() != null && !optionalForeignKeyNamingStrategy.isPresent()) {
				optionalForeignKeyNamingStrategy.set(entityMappingConfiguration.getForeignKeyNamingStrategy());
			}
		});
		this.foreignKeyNamingStrategy = optionalForeignKeyNamingStrategy.getOr(ForeignKeyNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ColumnNamingStrategy> optionalJoinColumnNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getJoinColumnNamingStrategy() != null && !optionalJoinColumnNamingStrategy.isPresent()) {
				optionalJoinColumnNamingStrategy.set(entityMappingConfiguration.getJoinColumnNamingStrategy());
			}
		});
		this.joinColumnNamingStrategy = optionalJoinColumnNamingStrategy.getOr(ColumnNamingStrategy.JOIN_DEFAULT);
		
		org.gama.lang.Nullable<AssociationTableNamingStrategy> optionalAssociationTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getAssociationTableNamingStrategy() != null && !optionalAssociationTableNamingStrategy.isPresent()) {
				optionalAssociationTableNamingStrategy.set(entityMappingConfiguration.getAssociationTableNamingStrategy());
			}
		});
		this.associationTableNamingStrategy = optionalAssociationTableNamingStrategy.getOr(AssociationTableNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ElementCollectionTableNamingStrategy> optionalElementCollectionTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getElementCollectionTableNamingStrategy() != null && !optionalElementCollectionTableNamingStrategy.isPresent()) {
				optionalElementCollectionTableNamingStrategy.set(entityMappingConfiguration.getElementCollectionTableNamingStrategy());
			}
		});
		this.elementCollectionTableNamingStrategy = optionalElementCollectionTableNamingStrategy.getOr(ElementCollectionTableNamingStrategy.DEFAULT);
	}
	
	void addIdentificationToMapping(Identification identification, Iterable<Mapping> mappings) {
		mappings.forEach(mapping -> {
			Column primaryKey = (Column) Iterables.first(mapping.getTargetTable().getPrimaryKey().getColumns());
			mapping.getMapping().put(identification.getIdAccessor(), primaryKey);
		});
	}
	
	/**
	 * Creates primary keys on given tables according to given identification
	 * 
	 * @param identification informations that allow to create primary keys
	 * @param joinedTables target tables on which primary keys must be added
	 */
	void addPrimarykeys(Identification identification, Set<Table> joinedTables) {
		Table pkTable = this.tableMap.get(identification.identificationDefiner);
		if (pkTable == null) {
			// Should not happen except during this class development
			throw new IllegalArgumentException("Table for primary key wasn't found in given tables : looking for "
					+ this.tableNamingStrategy.giveName(identification.identificationDefiner.getEntityType())
					+ " in [" + new StringAppender().ccat(Iterables.collectToList(joinedTables, Table::getAbsoluteName), ", ") + "]");
		}
		
		AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(identification.getIdAccessor());
		Column primaryKey = pkTable.addColumn(columnNamingStrategy.giveName(accessorDefinition), accessorDefinition.getMemberType());
		primaryKey.setNullable(false);	// may not be necessary because of primary key, let for principle
		primaryKey.primaryKey();
		if (identification.getIdentifierPolicy() == AFTER_INSERT) {
			primaryKey.autoGenerated();
		}
		final Column[] previousPk = { primaryKey };
		joinedTables.stream().filter(not(primaryKey.getTable()::equals)).forEach(t -> {
			Column newColumn = t.addColumn(columnNamingStrategy.giveName(accessorDefinition), accessorDefinition.getMemberType());
			newColumn.setNullable(false);	// may not be necessary because of primary key, let for principle
			newColumn.primaryKey();
			previousPk[0] = newColumn;
		});
		
	}
	
	void addForeignKeys(Identification identification, Set<Table> joinedTables) {
		Table pkTable = this.tableMap.get(identification.identificationDefiner);
		if (pkTable == null) {
			// Should not happen except during this class development
			throw new IllegalArgumentException("Table for primary key wasn't found in given tables : looking for "
					+ this.tableNamingStrategy.giveName(identification.identificationDefiner.getEntityType())
					+ " in [" + new StringAppender().ccat(Iterables.collectToList(joinedTables, Table::getAbsoluteName), ", ") + "]");
		}

		Column primaryKey = (Column) Iterables.first(pkTable.getPrimaryKey().getColumns());
		final Column[] previousPk = { primaryKey };
		joinedTables.stream().filter(not(primaryKey.getTable()::equals)).forEach(t -> {
			Column currentPrimaryKey = (Column) Iterables.first(t.getPrimaryKey().getColumns());
			t.addForeignKey(foreignKeyNamingStrategy.giveName(currentPrimaryKey, previousPk[0]), currentPrimaryKey, previousPk[0]);
			previousPk[0] = currentPrimaryKey;
		});
		
	}
	
	/**
	 * Gives embedded (non relational) properties mapping, including those from super classes
	 *
	 * @return the mapping between property accessor and their column in target tables, never null
	 */
	@VisibleForTesting
	MappingPerTable collectEmbeddedMappingFromInheritance() {
		MappingPerTable result = new MappingPerTable();
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		
		class MappingCollector implements Consumer<EmbeddableMappingConfiguration> {
			
			private Table currentTable;
			
			private Map<IReversibleAccessor, Column> currentColumnMap = new HashMap<>();
			
			private Mapping currentMapping;
			
			private Object currentKey;
			
			private boolean mappedSuperClass = false;
			
			@Override
			public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
				Map<IReversibleAccessor, Column> propertiesMapping = beanMappingBuilder.build(
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
					currentMapping = result.add(currentKey == null ? embeddableMappingConfiguration : currentKey, this.currentTable, currentColumnMap, this.mappedSuperClass);
				} else {
					currentMapping = result.add(embeddableMappingConfiguration, this.currentTable, currentColumnMap, this.mappedSuperClass);
					currentMapping.getMapping().putAll(currentColumnMap);
				}
				((List<Linkage>) embeddableMappingConfiguration.getPropertiesMapping()).stream()
						.filter(Linkage::isSetByConstructor).map(Linkage::getAccessor).forEach(currentMapping.propertiesSetByConstructor::add);
			}
			
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
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
	
	@VisibleForTesting
	MappingPerTable collectEmbeddedMappingFromSubEntities() {
		MappingPerTable result = new MappingPerTable();
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		PolymorphismPolicy polymorphismPolicy = entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			if (polymorphismPolicy instanceof SingleTablePolymorphism) {
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = ((SingleTablePolymorphism<?, ?, ?>) polymorphismPolicy).getSubClasses();
				subClasses.forEach(o -> result.add(o, this.table, beanMappingBuilder.build(o.getPropertiesMapping(),
						this.table, this.columnBinderRegistry, columnNameProvider), false));
			} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = ((TablePerClassPolymorphism<?, ?>) polymorphismPolicy).getSubClasses();
				subClasses.forEach(o -> result.add(o, this.table, beanMappingBuilder.build(o.getPropertiesMapping(),
						this.table, this.columnBinderRegistry, columnNameProvider), false));
			} else if (polymorphismPolicy instanceof JoinedTablesPolymorphism) {
				JoinedTablesPolymorphism<?, ?> joinedTablesPolymorphismPolicy = (JoinedTablesPolymorphism<?, ?>) polymorphismPolicy;
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = joinedTablesPolymorphismPolicy.getSubClasses();
				subClasses.forEach(o -> {
					Table table = nullable(joinedTablesPolymorphismPolicy.giveTable(o)).getOr(() -> new Table(tableNamingStrategy.giveName(o.getEntityType())));
					result.add(o, table, beanMappingBuilder.build(o.getPropertiesMapping(), table, this.columnBinderRegistry, columnNameProvider), false);
				});
			}
		}
		return result;
	}
	
	/**
	 * Looks for identifier as well as policy by going up through inheritance hierarchy.
	 *
	 * @return a couple that defines identification of the mapping
	 * @throws UnsupportedOperationException when identifiation was not found, because it doesn't make sense to have an entity without identification
	 */
	@VisibleForTesting
	<T extends Table<?>> Identification determineIdentification() {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Mapped super class and inheritance are not supported when they are combined, please remove one of them");
		}
		if (entityMappingConfiguration.getIdentifierAccessor() != null && entityMappingConfiguration.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported : "
					+ Reflections.toString(entityMappingConfiguration.getEntityType()) + " defined identifier " + AccessorDefinition.toString(entityMappingConfiguration.getIdentifierAccessor())
					+ " while it inherits from " + Reflections.toString(entityMappingConfiguration.getInheritanceConfiguration().getConfiguration().getEntityType()));
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		EntityMappingConfiguration<? super C, I> configurationDefiningIdentification;
		if (entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			if (entityMappingConfiguration.getIdentifierPolicy() == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				throw newMissingIdentificationException();
			} else {
				// NB: identifierAccessor is expected to be non null since policy can't be declared without it
				configurationDefiningIdentification = entityMappingConfiguration;
			}
		} else {
			// hierarchy must be scanned to find the very first configuration that defines identification
			configurationDefiningIdentification = Iterables.last(entityMappingConfiguration.inheritanceIterable());
			if (configurationDefiningIdentification.getIdentifierPolicy() == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				throw newMissingIdentificationException();
			}
		}
		return new Identification(configurationDefiningIdentification);
	}
	
	private UnsupportedOperationException newMissingIdentificationException() {
		SerializableBiFunction<ColumnOptions, IdentifierPolicy, ColumnOptions> identifierMethodReference = ColumnOptions::identifier;
		Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityMappingConfiguration.getEntityType())
				+ ", please add one throught " + Reflections.toString(identifierSetter));
	}
	
	static <X, I, T extends Table> ClassMappingStrategy<X, I, T> createClassMappingStrategy(
			boolean isIdentifyingConfiguration,
			T targetTable,
			Map<? extends IReversibleAccessor, Column> mapping,
			ValueAccessPointSet propertiesSetByConstructor,
			Identification identification,
			GeneratedKeysReaderBuilder generatedKeysReaderBuilder,
			Class<X> beanType,
			@Nullable Function<Function<Column, Object>, X> beanFactory) {

		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<X, I> identifierInsertionManager = null;
		
		Column primaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
		IdentifierPolicy identifierPolicy = identification.getIdentificationDefiner().getIdentifierPolicy();
		IReversibleAccessor idAccessor = identification.getIdAccessor();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		Class identifierType = idDefinition.getMemberType();
		if (isIdentifyingConfiguration) {
			if (identifierPolicy == AFTER_INSERT) {
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new SinglePropertyIdAccessor<>(idAccessor),
						generatedKeysReaderBuilder.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
						primaryKey.getJavaType()
				);
			} else if (identifierPolicy instanceof ColumnOptions.BeforeInsertIdentifierPolicy) {
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(
						new SinglePropertyIdAccessor<>(idAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
			} else if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
				if (Identified.class.isAssignableFrom(idDefinition.getDeclaringClass()) && Identifier.class.isAssignableFrom(identifierType)) {
					identifierInsertionManager = new IdentifiedIdentifierManager<>(identifierType);
				} else {
					throw new NotYetSupportedOperationException(
							"Already-assigned identifier policy is only supported with entities that implement " + Reflections.toString(Identified.class));
				}
			}
		} else {
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(identifierType);
		}
		SimpleIdMappingStrategy<X, I> simpleIdMappingStrategy = new SimpleIdMappingStrategy<>(idAccessor, identifierInsertionManager,
				new SimpleIdentifierAssembler<>(primaryKey));
		
		if (beanFactory == null) {
			Constructor<X> constructorById = Reflections.findConstructor(beanType, idDefinition.getMemberType());
			if (constructorById == null) {
				Constructor<X> defaultConstructor = Reflections.findConstructor(beanType);
				if (defaultConstructor == null) {
					// we'll lately throw an exception (we could do it now) but the lack of constructor may be due to an abstract class in heritance
					// path which currently won't be instanciated at runtime (because its concrete subclass will be) so there's no reason to throw
					// the exception now
					beanFactory = c -> {
						throw new MappingConfigurationException("Entity class " + Reflections.toString(beanType) + " doesn't have a compatible accessible constructor,"
							+ " please implement a no-arg constructor or " + Reflections.toString(idDefinition.getMemberType()) + "-arg constructor");
					};
				} else {
					beanFactory = c -> Reflections.newInstance(defaultConstructor);
				}
			} else {
				beanFactory = c -> Reflections.newInstance(constructorById, c.apply(simpleIdMappingStrategy.getIdentifierAssembler().getColumn()));
			}
		}
		
		ClassMappingStrategy<X, I, T> result = new ClassMappingStrategy<X, I, T>(beanType, targetTable, (Map) mapping, simpleIdMappingStrategy, beanFactory);
		propertiesSetByConstructor.forEach(result::addPropertySetByConstructor);
		return result;
	}
	
	/**
	 * Identifier manager dedicated to {@link Identified} entities
	 * @param <C> entity type
	 * @param <I> identifier type
	 */
	private static class IdentifiedIdentifierManager<C, I> extends AlreadyAssignedIdentifierManager<C, I> {
		public IdentifiedIdentifierManager(Class<I> identifierType) {
			super(identifierType);
		}
		
		@Override
		public void setPersistedFlag(C e) {
			((Identified) e).getId().setPersisted();
		}
	}
	
	
	static class Identification {
		
		private final IReversibleAccessor idAccessor;
		private final IdentifierPolicy identifierPolicy;
		private final EntityMappingConfiguration identificationDefiner;
		
		
		public Identification(EntityMappingConfiguration identificationDefiner) {
			this.idAccessor = identificationDefiner.getIdentifierAccessor();
			this.identifierPolicy = identificationDefiner.getIdentifierPolicy();
			this.identificationDefiner = identificationDefiner;
		}
		
		public IReversibleAccessor getIdAccessor() {
			return idAccessor;
		}
		
		public IdentifierPolicy getIdentifierPolicy() {
			return identifierPolicy;
		}
		
		public EntityMappingConfiguration getIdentificationDefiner() {
			return identificationDefiner;
		}
	}
	
	@VisibleForTesting
	static class MappingPerTable {
		private final KeepOrderSet<Mapping> mappings = new KeepOrderSet<>();
		
		Mapping add(Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration,
				 Table table, Map<IReversibleAccessor, Column> mapping, boolean mappedSuperClass) {
			Mapping newMapping = new Mapping(mappingConfiguration, table, mapping, mappedSuperClass);
			this.mappings.add(newMapping);
			return newMapping;
		}
		
		Map<IReversibleAccessor, Column> giveMapping(Table table) {
			Mapping foundMapping = Iterables.find(this.mappings, m -> m.getTargetTable().equals(table));
			if (foundMapping == null) {
				throw new IllegalArgumentException("Can't find table '" + table.getAbsoluteName()
						+ "' in " + Iterables.collectToList(this.mappings, Functions.chain(Mapping::getTargetTable, Table::getAbsoluteName)).toString());
			}
			return foundMapping.mapping;
		}
		
		Set<Table> giveTables() {
			return Iterables.collect(this.mappings, Mapping::getTargetTable, HashSet::new);
		}
		
		public KeepOrderSet<Mapping> getMappings() {
			return mappings;
		}
		
		static class Mapping {
			private final Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration;
			private final Table targetTable;
			private final Map<IReversibleAccessor, Column> mapping;
			private final ValueAccessPointSet propertiesSetByConstructor = new ValueAccessPointSet();
			private final boolean mappedSuperClass;
			
			Mapping(Object mappingConfiguration, Table targetTable, Map<IReversibleAccessor, Column> mapping, boolean mappedSuperClass) {
				this.mappingConfiguration = mappingConfiguration;
				this.targetTable = targetTable;
				this.mapping = mapping;
				this.mappedSuperClass = mappedSuperClass;
			}
			
			public Object getMappingConfiguration() {
				return mappingConfiguration;
			}
			
			public boolean isMappedSuperClass() {
				return mappedSuperClass;
			}
			
			public EmbeddableMappingConfiguration giveEmbeddableConfiguration() {
				return (EmbeddableMappingConfiguration) (this.mappingConfiguration instanceof EmbeddableMappingConfiguration
						? this.mappingConfiguration
						: (this.mappingConfiguration instanceof EntityMappingConfiguration ?
						((EntityMappingConfiguration) this.mappingConfiguration).getPropertiesMapping()
						: null));
			}
			
			private Table getTargetTable() {
				return targetTable;
			}
			
			public Map<IReversibleAccessor, Column> getMapping() {
				return mapping;
			}
		}
	}
	
	@FunctionalInterface
	private interface GeneratedKeysReaderBuilder {
		GeneratedKeysReader buildGeneratedKeysReader(String keyName, Class columnType);
	}
}
