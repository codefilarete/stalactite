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
import java.util.Objects;
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
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Functions;
import org.gama.lang.function.Hanger.Holder;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnOptions;
import org.gama.stalactite.persistence.engine.ColumnOptions.AlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.engine.ColumnOptions.BeforeInsertIdentifierPolicy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PersisterBuilder;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.VersioningStrategy;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.engine.runtime.EntityIsManagedByPersisterAsserter;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeSelectBuilder;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.OptimizedUpdatePersister;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport.EntityGraphNode;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
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
	public IEntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect,
															  IConnectionConfiguration connectionConfiguration,
															  PersisterRegistry persisterRegistry,
															  @Nullable Table table) {
		boolean isInitiator = ENTITY_CANDIDATES.get() == null;
		
		if (isInitiator) {
			ENTITY_CANDIDATES.set(new HashSet<>());
		}
		
		try {
			// If a persister already exists for the type we return it : case of graph that declares twice / several times same mapped type 
			// WARN : this does not take mapping configuration differences into account, so if configuration is different from previous one, since
			// no check is done, then the very first persister is returned
			IEntityPersister<C, Object> existingPersister = persisterRegistry.getPersister(this.entityMappingConfiguration.getEntityType());
			if (existingPersister != null) {
				// we can cast because all persisters we registered implement the interface
				return (IEntityConfiguredJoinedTablesPersister<C, I>) existingPersister;
			}
			return doBuild(table, dialect::buildGeneratedKeysReader, dialect, connectionConfiguration, persisterRegistry);
		} finally {
			if (isInitiator) {
				ENTITY_CANDIDATES.remove();
			}
		}
	}
	
	private IEntityConfiguredJoinedTablesPersister<C, I> doBuild(@Nullable Table table,
																 GeneratedKeysReaderBuilder generatedKeysReaderBuilder,
																 Dialect dialect,
																 IConnectionConfiguration connectionConfiguration,
																 PersisterRegistry persisterRegistry) {
		init(dialect.getColumnBinderRegistry(), table);
		
		mapEntityConfigurationPerTable();
		
		Identification identification = determineIdentification();
		
		// collecting mapping from inheritance
		MappingPerTable inheritanceMappingPerTable = collectEmbeddedMappingFromInheritance();
		// TODO: check if this can be done just before cascade ones addition
		inheritanceMappingPerTable.getMappings().forEach(mapping -> {
			if (mapping.getMappingConfiguration() instanceof EntityMappingConfiguration) {
				((EntityMappingConfiguration<C, I>) mapping.getMappingConfiguration()).getOneToOnes().forEach(cascadeOne -> {
					if (!cascadeOne.isRelationOwnedByTarget()) {
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
		// determining insertion manager must be done AFTER primary key addition, else it would fall into NullPointerException
		determineIdentifierManager(identification, inheritanceMappingPerTable, identification.getIdAccessor(), generatedKeysReaderBuilder);
		
		// Creating main persister 
		Mapping mainMapping = Iterables.first(inheritanceMappingPerTable.getMappings());
		JoinedTablesPersister<C, I, Table> mainPersister = buildMainPersister(identification, mainMapping, dialect, connectionConfiguration);
		ENTITY_CANDIDATES.get().add(mainPersister.getMappingStrategy().getClassToPersist());
		
		// registering relations on parent entities
		// WARN : this MUST BE DONE BEFORE POLYMORPHISM HANDLING because it needs them to create adhoc joins on sub entities tables 
		inheritanceMappingPerTable.getMappings().stream()
				.map(Mapping::getMappingConfiguration)
				.filter(EntityMappingConfiguration.class::isInstance)
				.map(EntityMappingConfiguration.class::cast)
				.forEach(entityMappingConfiguration -> registerRelationCascades(entityMappingConfiguration, dialect, connectionConfiguration, persisterRegistry, mainPersister));
		
		IEntityConfiguredJoinedTablesPersister<C, I> result = mainPersister;
		// polymorphism handling
		PolymorphismPolicy polymorphismPolicy = this.entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismBuilder<C, I, Table> polymorphismBuilder;
			if (polymorphismPolicy instanceof SingleTablePolymorphism) {
				polymorphismBuilder = new SingleTablePolymorphismBuilder<>((SingleTablePolymorphism<C, I, ?>) polymorphismPolicy,
						identification, mainPersister, mainMapping, this.columnBinderRegistry, this.columnNameProvider,
						this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy, this.joinColumnNamingStrategy,
						this.associationTableNamingStrategy);
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
						identification, mainPersister, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy,
						this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy, this.joinColumnNamingStrategy,
						this.associationTableNamingStrategy) {
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
			} else {
				// this exception is more to satisfy Sonar than for real case
				throw new NotImplementedException("Given policy is not implemented : " + polymorphismPolicy);
			}
			result = polymorphismBuilder.build(dialect, connectionConfiguration, persisterRegistry);
			// we transfert listeners by principle and in particular for already-assigned mark-as-persisted mecanism and relation cascade triggering
			result.addInsertListener(mainPersister.getPersisterListener().getInsertListener());
			result.addUpdateListener(mainPersister.getPersisterListener().getUpdateListener());
			result.addSelectListener(mainPersister.getPersisterListener().getSelectListener());
			result.addDeleteListener(mainPersister.getPersisterListener().getDeleteListener());
			result.addDeleteByIdListener(mainPersister.getPersisterListener().getDeleteByIdListener());
		}
		
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forgot to / can't mark it as such
		if (entityMappingConfiguration.getIdentifierPolicy() instanceof ColumnOptions.AlreadyAssignedIdentifierPolicy) {
			Consumer<C> asPersistedMarker = ((AlreadyAssignedIdentifierPolicy<C, I>) entityMappingConfiguration.getIdentifierPolicy()).getMarkAsPersistedFunction();
			result.addSelectListener(new SelectListener<C, I>() {
				@Override
				public void afterSelect(Iterable<? extends C> result) {
					Iterables.filter(result, Objects::nonNull).forEach(asPersistedMarker);
				}
			});
		}
		
		// parent persister must be kept in ascending order for further treatments
		Iterator<Mapping> mappings = Iterables.filter(Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().asSet()),
				m -> !mainMapping.equals(m) && !m.mappedSuperClass);
		KeepOrderSet<JoinedTablesPersister<C, I, Table>> parentPersisters = buildParentPersisters(() -> mappings,
				identification, mainPersister, dialect, connectionConfiguration
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister, parentPersisters);
		
		handleVersioningStrategy(mainPersister);
		
		// we wrap final result with some transversal features
		// NB: Order of wrap is important due to invokation of instance methods with code like "this.doSomething(..)" in particular with OptimizedUpdatePersister
		// which internaly calls update(C, C, boolean) on update(id, Consumer): the latter method is not listened by EntityIsManagedByPersisterAsserter
		// (because it has no purpose since entity is not given as argument) but update(C, C, boolean) is and should be, that is not the case if
		// EntityIsManagedByPersisterAsserter is done first since OptimizedUpdatePersister invokes itself with "this.update(C, C, boolean)"
		OptimizedUpdatePersister<C, I> optimizedPersister = new OptimizedUpdatePersister<>(
				new EntityIsManagedByPersisterAsserter<>(result));
		persisterRegistry.addPersister(optimizedPersister);
		parentPersisters.forEach(persisterRegistry::addPersister);
		
		return optimizedPersister;
	}
	
	private <T extends Table> void registerRelationCascades(EntityMappingConfiguration<C, I> entityMappingConfiguration,
															Dialect dialect,
															IConnectionConfiguration connectionConfiguration,
															PersisterRegistry persisterRegistry,
															JoinedTablesPersister<C, I, T> sourcePersister) {
		for (CascadeOne<C, ?, ?> cascadeOne : entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(dialect,
					connectionConfiguration,
					persisterRegistry,
					new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			cascadeOneConfigurer.appendCascade(cascadeOne, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy);
		}
		for (CascadeMany<C, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(
					dialect,
					connectionConfiguration,
					persisterRegistry,
					new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.associationTableNamingStrategy);
		}
		registerRelationsInGraph(entityMappingConfiguration, sourcePersister.getCriteriaSupport().getRootConfiguration(), persisterRegistry);
		
		// taking element collections into account
		for (ElementCollectionLinkage<C, ?, ? extends Collection> elementCollection : entityMappingConfiguration.getElementCollections()) {
			ElementCollectionCascadeConfigurer elementCollectionCascadeConfigurer = new ElementCollectionCascadeConfigurer(dialect, connectionConfiguration);
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
	 * @param persisterRegistry a per-entity {@link org.gama.stalactite.persistence.mapping.IEntityMappingStrategy} registry 
	 */
	private void registerRelationsInGraph(EntityMappingConfiguration configurationSupport, EntityGraphNode target, PersisterRegistry persisterRegistry) {
		List<CascadeMany> oneToManys = configurationSupport.getOneToManys();
		oneToManys.forEach((CascadeMany cascadeMany) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeMany.getCollectionProvider(),
					((IConfiguredPersister) persisterRegistry.getPersister(cascadeMany.getTargetMappingConfiguration().getEntityType())).getMappingStrategy());
			registerRelationsInGraph(cascadeMany.getTargetMappingConfiguration(), entityGraphNode, persisterRegistry);
		});
		List<CascadeOne> oneToOnes = configurationSupport.getOneToOnes();
		oneToOnes.forEach((CascadeOne cascadeOne) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeOne.getTargetProvider(),
					((IConfiguredPersister) persisterRegistry.getPersister(cascadeOne.getTargetMappingConfiguration().getEntityType())).getMappingStrategy());
			registerRelationsInGraph(cascadeOne.getTargetMappingConfiguration(), entityGraphNode, persisterRegistry);
		});
	}
	
	private <T extends Table> KeepOrderSet<JoinedTablesPersister<C, I, Table>> buildParentPersisters(Iterable<Mapping> mappings,
																						 Identification identification,
																						 JoinedTablesPersister<C, I, T> mainPersister,
																						 Dialect dialect,
																						 IConnectionConfiguration connectionConfiguration) {
		KeepOrderSet<JoinedTablesPersister<C, I, Table>> parentPersisters = new KeepOrderSet<>();
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
					mapping.giveEmbeddableConfiguration().getBeanType(),
					null);
			
			JoinedTablesPersister<C, I, Table> currentPersister = new JoinedTablesPersister<>(currentMappingStrategy, dialect, connectionConfiguration);
			parentPersisters.add(currentPersister);
			// a join is necessary to select entity, only if target table changes
			if (!currentPersister.getMainTable().equals(currentTable.get())) {
				mainPersister.getEntityMappingStrategyTreeSelectExecutor().addComplementaryJoin(EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME, currentMappingStrategy,
					superclassPK, subclassPK);
				currentTable.set(currentPersister.getMainTable());
			}
		});
		return parentPersisters;
	}
	
	private <T extends Table> JoinedTablesPersister<C, I, T> buildMainPersister(Identification identification,
																				Mapping mapping,
																				Dialect dialect,
																				IConnectionConfiguration connectionConfiguration) {
		EntityMappingConfiguration mainMappingConfiguration = (EntityMappingConfiguration) mapping.mappingConfiguration;
		ClassMappingStrategy<C, I, T> parentMappingStrategy = createClassMappingStrategy(
				identification.getIdentificationDefiner().getPropertiesMapping() == mainMappingConfiguration.getPropertiesMapping(),
				mapping.targetTable,
				mapping.mapping,
				mapping.propertiesSetByConstructor,
				identification,
				mainMappingConfiguration.getEntityType(),
				mainMappingConfiguration.getEntityFactory());
		return new JoinedTablesPersister<>(parentMappingStrategy, dialect, connectionConfiguration);
	}
	
	interface PolymorphismBuilder<C, I, T extends Table> {
		
		IEntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, IConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry);
	}
	
	/**
	 * 
	 * @param mainPersister main persister
	 * @param superPersisters persisters in ascending order
	 */
	private void addCascadesBetweenChildAndParentTable(JoinedTablesPersister<C, I, ? extends Table> mainPersister,
													   KeepOrderSet<JoinedTablesPersister<C, I, Table>> superPersisters) {
		// we add cascade only on persister with different table : we keep the "lowest" one because it gets all inherited properties,
		// upper ones are superfluous
		KeepOrderSet<JoinedTablesPersister<C, I, Table>> superPersistersWithChangingTable = new KeepOrderSet<>();
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
		
		List<JoinedTablesPersister<C, I, Table>> copy = Iterables.copy(superPersistersWithChangingTable);
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
		
		org.gama.lang.Nullable<TableNamingStrategy> optionalTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getTableNamingStrategy() != null && !optionalTableNamingStrategy.isPresent()) {
				optionalTableNamingStrategy.set(configuration.getTableNamingStrategy());
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
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getForeignKeyNamingStrategy() != null && !optionalForeignKeyNamingStrategy.isPresent()) {
				optionalForeignKeyNamingStrategy.set(configuration.getForeignKeyNamingStrategy());
			}
		});
		this.foreignKeyNamingStrategy = optionalForeignKeyNamingStrategy.getOr(ForeignKeyNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ColumnNamingStrategy> optionalJoinColumnNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getJoinColumnNamingStrategy() != null && !optionalJoinColumnNamingStrategy.isPresent()) {
				optionalJoinColumnNamingStrategy.set(configuration.getJoinColumnNamingStrategy());
			}
		});
		this.joinColumnNamingStrategy = optionalJoinColumnNamingStrategy.getOr(ColumnNamingStrategy.JOIN_DEFAULT);
		
		org.gama.lang.Nullable<AssociationTableNamingStrategy> optionalAssociationTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getAssociationTableNamingStrategy() != null && !optionalAssociationTableNamingStrategy.isPresent()) {
				optionalAssociationTableNamingStrategy.set(configuration.getAssociationTableNamingStrategy());
			}
		});
		this.associationTableNamingStrategy = optionalAssociationTableNamingStrategy.getOr(AssociationTableNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ElementCollectionTableNamingStrategy> optionalElementCollectionTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getElementCollectionTableNamingStrategy() != null && !optionalElementCollectionTableNamingStrategy.isPresent()) {
				optionalElementCollectionTableNamingStrategy.set(configuration.getElementCollectionTableNamingStrategy());
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
	
	/**
	 * Visits parent {@link EntityMappingConfiguration} of current entity mapping configuration (including itself), this is an optional operation
	 * because current configuration may not have a direct entity ancestor.
	 * Then visits mapped super classes as {@link EmbeddableMappingConfiguration} of the last visited {@link EntityMappingConfiguration}, optional
	 * operation too.
	 * This is because inheritance tree can only have 2 paths :
	 * - first an optional inheritance from some other entity
	 * - then an optional inheritance from some mapped sur class
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
	 * Looks for identifier as well as policy by going up through inheritance hierarchy.
	 *
	 * @return a couple that defines identification of the mapping
	 * @throws UnsupportedOperationException when identifiation was not found, because it doesn't make sense to have an entity without identification
	 */
	private Identification determineIdentification() {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		if (entityMappingConfiguration.getIdentifierAccessor() != null && entityMappingConfiguration.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported : "
					+ Reflections.toString(entityMappingConfiguration.getEntityType()) + " defines identifier "
					+ AccessorDefinition.toString(entityMappingConfiguration.getIdentifierAccessor())
					+ " while it inherits from " + Reflections.toString(entityMappingConfiguration.getInheritanceConfiguration().getConfiguration().getEntityType()));
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		final Holder<EntityMappingConfiguration<? super C, I>> configurationDefiningIdentification = new Holder<>();
		// hierarchy must be scanned to find the very first configuration that defines identification
		visitInheritedEntityMappingConfigurations(entityConfiguration -> {
			if (entityConfiguration.getIdentifierPolicy() != null) {
				if (configurationDefiningIdentification.get() != null) {
					throw new UnsupportedOperationException("Identifier policy is defined twice in hierachy : first by "
							+ Reflections.toString(configurationDefiningIdentification.get().getEntityType())
							+ ", then by " + Reflections.toString(entityConfiguration.getEntityType()));
				} else {
					configurationDefiningIdentification.set(entityConfiguration);
				}
			}
		});
		EntityMappingConfiguration<? super C, I> foundConfiguration = configurationDefiningIdentification.get();
		if (foundConfiguration == null) {
			throw newMissingIdentificationException();
		}
		return new Identification(foundConfiguration);
	}
	
	/**
	 * Determines {@link IdentifierInsertionManager} for current configuration as weel as its whole inheritance configuration.
	 * The result is set in given {@link Identification}. Could have been done on a separatate object but it would have complexified some method
	 * signature, and {@link Identification} is a good place for it.
	 * 
	 * @param identification given to know expected policy, and to set result in it
	 * @param mappingPerTable necessary to get table and primary key to be read in after-insert policy
	 * @param idAccessor id accessor to get and set identifier on entity (except for already-assigned strategy)
	 * @param generatedKeysReaderBuilder reader for {@link IdentifierPolicy#AFTER_INSERT}
	 * @param <X> entity type that defines identifier manager, used as internal, may be C or one of its ancestor
	 */
	private <X> void determineIdentifierManager(
			Identification identification,
			MappingPerTable mappingPerTable,
			IReversibleAccessor idAccessor,
			GeneratedKeysReaderBuilder generatedKeysReaderBuilder) {
		IdentifierInsertionManager<X, I> identifierInsertionManager = null;
		IdentifierPolicy identifierPolicy = identification.getIdentifierPolicy();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		Class<I> identifierType = idDefinition.getMemberType();
		if (identifierPolicy == AFTER_INSERT) {
			// with identifier set by database generated key, identifier must be retrieved as soon as possible which means by the very first
			// persister, which is current one, which is the first in order of mappings
			Table targetTable = Iterables.first(mappingPerTable.getMappings()).targetTable;
			Column primaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
			identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
					new SinglePropertyIdAccessor<>(idAccessor),
					generatedKeysReaderBuilder.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
					primaryKey.getJavaType()
			);
		} else if (identifierPolicy instanceof ColumnOptions.BeforeInsertIdentifierPolicy) {
			identifierInsertionManager = new BeforeInsertIdentifierManager<>(
					new SinglePropertyIdAccessor<>(idAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
		} else if (identifierPolicy instanceof ColumnOptions.AlreadyAssignedIdentifierPolicy) {
			AlreadyAssignedIdentifierPolicy<X, I> alreadyAssignedPolicy = (AlreadyAssignedIdentifierPolicy<X, I>) identifierPolicy;
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
					identifierType,
					alreadyAssignedPolicy.getMarkAsPersistedFunction(),
					alreadyAssignedPolicy.getIsPersistedFunction());
		}
		
		// Treating configuration that are not the identifying one : they get an already-assigned identifier manager
		Function<X, Boolean> isPersistedFunction;
		if (!identifierType.isPrimitive()) {
			isPersistedFunction = c -> idAccessor.get(c) != null;
		} else {
			isPersistedFunction = c ->  Reflections.PRIMITIVE_DEFAULT_VALUES.get(identifierType) == idAccessor.get(c);
		}
		AlreadyAssignedIdentifierManager<X, I> fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType, c -> {}, isPersistedFunction);
		identification.insertionManager = (IdentifierInsertionManager<Object, Object>) identifierInsertionManager;
		identification.fallbackInsertionManager = (AlreadyAssignedIdentifierManager<Object, Object>) fallbackMappingIdentifierManager;
	}
	
	private UnsupportedOperationException newMissingIdentificationException() {
		SerializableBiFunction<ColumnOptions, IdentifierPolicy, ColumnOptions> identifierMethodReference = ColumnOptions::identifier;
		Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityMappingConfiguration.getEntityType())
				+ ", please add one throught " + Reflections.toString(identifierSetter));
	}
	
	/**
	 * 
	 * @param isIdentifyingConfiguration
	 * @param targetTable
	 * @param mapping
	 * @param propertiesSetByConstructor
	 * @param identification
	 * @param beanType
	 * @param beanFactory optional, if null default beantype constructor will be used
	 * @param <X>
	 * @param <I>
	 * @param <T>
	 * @return
	 */
	static <X, I, T extends Table> ClassMappingStrategy<X, I, T> createClassMappingStrategy(
			boolean isIdentifyingConfiguration,
			T targetTable,
			Map<? extends IReversibleAccessor, Column> mapping,
			ValueAccessPointSet propertiesSetByConstructor,
			Identification identification,
			Class<X> beanType,
			@Nullable Function<Function<Column, Object>, X> beanFactory) {

		Column primaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
		IReversibleAccessor idAccessor = identification.getIdAccessor();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<X, I> identifierInsertionManager = (IdentifierInsertionManager<X, I>) (isIdentifyingConfiguration
				? identification.insertionManager
				: identification.fallbackInsertionManager);
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
	
	static class Identification {
		
		private final IReversibleAccessor idAccessor;
		private final IdentifierPolicy identifierPolicy;
		private final EntityMappingConfiguration identificationDefiner;
		
		/** Insertion manager for {@link ClassMappingStrategy} that owns identifier policy */
		private IdentifierInsertionManager<Object, Object> insertionManager;
		/** Insertion manager for {@link ClassMappingStrategy} that doesn't own identifier policy : they get an already-assigned one */
		private AlreadyAssignedIdentifierManager<Object, Object> fallbackInsertionManager;
		
		
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
