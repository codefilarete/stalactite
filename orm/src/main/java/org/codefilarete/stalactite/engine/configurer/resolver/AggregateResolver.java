package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.EntityRelation;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.configurer.model.RelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.AggregateElementCollectionAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.manytomany.AggregateManyToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.manytomany.ManyToManyResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.manytoone.AggregateManyToOneAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.manytoone.ManyToOneResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.AggregateMapAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.map.MapResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.AggregateOneToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.OneToManyResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.AggregateOneToOneAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.OneToOneResolver;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Experimental;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateResolver {
	
	private final PersistenceContext persistenceContext;
	private final AggregateMetadataResolver aggregateMetadataResolver;
	private final PersisterRegistry persisterRegistry;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final AggregateOneToOneAppender oneToOneAppender;
	private final AggregateOneToManyAppender oneToManyAppender;
	private final AggregateManyToManyAppender manyToManyAppender;
	private final AggregateManyToOneAppender manyToOneAppender;
	private final AggregateElementCollectionAppender elementCollectionAppender;
	private final AggregateMapAppender mapAppender;
	
	private final OneToOneResolver oneToOneResolver;
	private final OneToManyResolver oneToManyResolver;
	private final ManyToManyResolver manyToManyResolver;
	private final ManyToOneResolver manyToOneResolver;
	private final ElementCollectionResolver elementCollectionResolver;
	private final MapResolver mapResolver;
	
	public AggregateResolver(PersistenceContext persistenceContext) {
		this(persistenceContext, persistenceContext.getPersisterRegistry());
	}
	
	AggregateResolver(PersistenceContext persistenceContext, PersisterRegistry persisterRegistry) {
		this.persistenceContext = persistenceContext;
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.oneToOneAppender = new AggregateOneToOneAppender();
		this.oneToManyAppender = new AggregateOneToManyAppender();
		this.manyToManyAppender = new AggregateManyToManyAppender(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToOneAppender = new AggregateManyToOneAppender();
		this.elementCollectionAppender = new AggregateElementCollectionAppender();
		this.mapAppender = new AggregateMapAppender();
		
		this.oneToOneResolver = new OneToOneResolver(skeletonAggregateResolver);
		this.oneToManyResolver = new OneToManyResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToManyResolver = new ManyToManyResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToOneResolver = new ManyToOneResolver(skeletonAggregateResolver);
		this.elementCollectionResolver = new ElementCollectionResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.mapResolver = new MapResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public <C, I> EntityPersister<C, I> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		AbstractEntity<C, I, ?> rootEntity = aggregateMetadataResolver.resolve(rootConfiguration);
		return build(rootEntity);
	}
	
	<C, I> EntityPersister<C, I> build(AbstractEntity<C, I, ?> rootEntity) {
		// all this is left for compatibility with existing persister builders mechanism
		// it should be removed (or replaced by a close mechanism) at the very end of the implementation of the new persister build mechanism
		PersisterBuilderContext persisterBuilderContext = PersisterBuilderContext.CURRENT.get();
		boolean isInitiator = false;
		if (persisterBuilderContext == null) {
			persisterBuilderContext = new PersisterBuilderContext(persisterRegistry);
			PersisterBuilderContext.CURRENT.set(persisterBuilderContext);
			isInitiator = true;
		}
		
		try {
			EntityPersister<C, I> result = buildPersister(rootEntity);
			// making aggregate persister available for external usage
			persisterRegistry.addPersister(result);
			if (isInitiator) {
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
			}
			return result;
		} finally {
			if (isInitiator) {
				PersisterBuilderContext.CURRENT.remove();
			}
		}
	}
	
	private <C, I, T extends Table<T>>
	EntityPersister<C, I> buildPersister(AbstractEntity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		EntityWriteExecutor<C, I> rootWriter = null;
		CreatedPersisterCollector<C, I> rootPersisterCollector = new CreatedPersisterCollector<>();
		if (rootEntity instanceof Entity) {
			rootWriter = skeletonAggregateResolver.buildPersister((Entity<C, I, T>) rootEntity, rootPersisterCollector);
		} else {
			
		}
		
		Map<MappingJoin<?, ?, ?>, Object> createdPersistersMap = (Map) appendWriteCascades(rootEntity, rootWriter);
		createdPersistersMap.put(null, rootPersisterCollector);
		EntityReader<C, I, ?> aggregateReader = new EntityReader<>(rootWriter.<T>getMapping(),
				persistenceContext.getConnectionProvider(),
				persistenceContext.getDialect());
		composeLoadTree(rootEntity, aggregateReader, createdPersistersMap);
		
		DelegatingReadWriteEntityExecutor<C, I> almostResult = new DelegatingReadWriteEntityExecutor<>(rootWriter, aggregateReader);
		
		Set<Table<?>> tables = collectTables(rootEntity);
		
		ConfiguredPersister<C, I> result = wrapToConfiguredPersister(almostResult, tables);
		
		return result;
	}
	
	private <C, I> ConfiguredPersister<C, I> wrapToConfiguredPersister(DelegatingReadWriteEntityExecutor<C, I> almostResult, Set<? extends Table<?>> tables) {
		return new DelegatingConfiguredPersister<>(almostResult, tables);
	}
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	Map<MappingJoin<LEFTTABLE, RIGHTTABLE, ?>, Object> appendWriteCascades(AbstractEntity<SRC, SRCID, LEFTTABLE> rootEntity,
	                                                                       EntityWriteExecutor<SRC, SRCID> aggregatePersister) {
		
		// Iterating over all the one-to-many relations of the tree (starting from given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some AssemblyPoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<AssemblyPoint<?, ?, ?, ?>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new AssemblyPoint<>(rootEntity, aggregatePersister, null, null));
		
		Map<MappingJoin<LEFTTABLE, RIGHTTABLE, ?>, Object> result = new HashMap<>();
		
		while (!relationStack.isEmpty()) {
			AssemblyPoint<?, ?, ?, ?> assemblyPawn = relationStack.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						EntityWriteExecutor<SRC, SRCID> relationOwnerPersister = (EntityWriteExecutor<SRC, SRCID>) assemblyPawn.getRelationOwnerPersister();
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector,
									localRelation.getAccessor());
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new AssemblyPoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister(), null, null));
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new AssemblyPoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister(), null, null));
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new AssemblyPoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister(), null, null));
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new AssemblyPoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister(), null, null));
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							CreatedPersisterCollector<ElementRecord<TRGT, SRCID>, ElementRecord<TRGT, SRCID>> createdPersisterCollector = new CreatedPersisterCollector<>();
							ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>> localRelation = (ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn;
							elementCollectionResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?> localRelation = (ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?>) relationPawn;
							MapCreatedPersisterCollector<SRCID, Object, Object, Object, Object, RIGHTTABLE, Object, Object> mapCreatedPersisterCollector = new MapCreatedPersisterCollector<>();
							mapResolver.resolve(
									localRelation,
									relationOwnerPersister,
									mapCreatedPersisterCollector);
							result.put(localRelation, mapCreatedPersisterCollector);
							if (localRelation.getKeyEntityDefinition() != null) {
								relationStack.add(new AssemblyPoint(localRelation.getKeyEntityDefinition().getEntity(), mapCreatedPersisterCollector.getKeyPersisterCollector().getPersister(), null, null));
							}
							if (localRelation.getValueEntityDefinition() != null) {
								relationStack.add(new AssemblyPoint(localRelation.getValueEntityDefinition().getEntity(), mapCreatedPersisterCollector.getValuePersisterCollector().getPersister(), null, null));
							}
						}
					});
		}
		
		return result;
	}
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void composeLoadTree(AbstractEntity<SRC, SRCID, LEFTTABLE> rootEntity,
	                     EntityReader<SRC, SRCID, ?> aggregatePersister,
	                     Map<MappingJoin<?, ?, ?>, Object> createdPersisters) {
		
		Queue<AssemblyPoint2<?, ?, ?, ?>> relationStack2 = new ArrayDeque<>();
		relationStack2.add(new AssemblyPoint2<>(rootEntity, aggregatePersister, ROOT_JOIN_NAME, null));
		new SkeletonAggregateAppender()
				.appendInheritance((CreatedPersisterCollector<SRC, SRCID>) createdPersisters.get(null), aggregatePersister.getEntityJoinTree());
		
		while (!relationStack2.isEmpty()) {
			AssemblyPoint2<?, ?, ?, ?> assemblyPawn = relationStack2.poll();
			EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister = (EntityReader<SRC, SRCID, LEFTTABLE>) assemblyPawn.getRelationOwnerPersister();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							EntityReader<TRGT, TRGTID, ?> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> assemblyPoint2 = oneToOneAppender.append(
									localRelation,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack2.add(assemblyPoint2);
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> assemblyPoint2 = oneToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack2.add(assemblyPoint2);
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							
							AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> assemblyPoint2 = manyToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack2.add(assemblyPoint2);
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> assemblyPoint2 = manyToOneAppender.append(
									localRelation,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							relationStack2.add(assemblyPoint2);
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							EntityReadWriteExecutor<TRGT, TRGTID> persister = localCreatedPersistor.getPersister();
							elementCollectionAppender.append(
									(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn,
									aggregatePersister.getEntityJoinTree(),
									(ElementCollectionResolver.ElementRecordPersister<TRGT, SRCID, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) persister,
									assemblyPawn.getParentJoinPoint()
							);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							MapCreatedPersisterCollector mapCreatedPersisterCollector = (MapCreatedPersisterCollector) createdPersisters.get(relationPawn);
							ResolvedMapRelation localRelation = (ResolvedMapRelation) relationPawn;
							graftMapRelation(localRelation, aggregatePersister.getEntityJoinTree(), assemblyPawn.getParentJoinPoint(), (EntityReader<SRC, SRCID, LEFTTABLE>) assemblyPawn.getRelationOwnerPersister(), mapCreatedPersisterCollector);
						}
					});
		}
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	void graftMapRelation(ResolvedMapRelation relation,
	                      EntityJoinTree<SRC, SRCID> aggregateTree,
	                      String mountPoint,
	                      EntityReader<SRC, SRCID, LEFTTABLE> relationOwnerPersister,
	                      MapCreatedPersisterCollector collectedPersisters) {
		MapCreatedPersisterCollector<SRCID, K, KID, V, VID, MAPTABLE, X, Y> typedMapCreatedPersisterCollector = (MapCreatedPersisterCollector<SRCID, K, KID, V, VID, MAPTABLE, X, Y>) collectedPersisters;
		ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> typedRelation = (ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE>) relation;
		EntityReader<K, KID, KTABLE> keyEntityReader = null;
		if (typedMapCreatedPersisterCollector.getKeyPersisterCollector() != null) {
			keyEntityReader = new EntityReader<>(
					typedMapCreatedPersisterCollector.getKeyPersisterCollector().getPersister().<KTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
		}
		EntityReader<V, VID, VTABLE> valueEntityReader = null;
		if (typedMapCreatedPersisterCollector.getValuePersisterCollector() != null) {
			valueEntityReader = new EntityReader<>(
					typedMapCreatedPersisterCollector.getValuePersisterCollector().getPersister().<VTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
		}
		mapAppender.append(
				typedRelation,
				aggregateTree,
				relationOwnerPersister,
				mountPoint,
				typedMapCreatedPersisterCollector.getKeyValueRecordPersister(),
				keyEntityReader,
				valueEntityReader
		);
	}
	
	private <C, I, T extends Table<T>> Set<Table<?>> collectTables(AbstractEntity<C, I, T> rootEntity) {
		Set<Table<?>> tables = new HashSet<>();
		Queue<AbstractEntity<?, ?, ?>> relationStack = new ArrayDeque<>();
		relationStack.add(rootEntity);
		while (!relationStack.isEmpty()) {
			AbstractEntity<?, ?, ?> entity = relationStack.poll();
			tables.add(entity.getTable());
			entity.getRelations().forEach(relation -> {
				RelationJoin relationJoin = relation.getJoin();
				if (relationJoin instanceof DirectRelationJoin) {
					DirectRelationJoin directRelationJoin = (DirectRelationJoin) relationJoin;
					tables.add((Table<?>) directRelationJoin.getLeftKey().getTable());
					tables.add((Table<?>) directRelationJoin.getRightKey().getTable());
				} else if (relationJoin instanceof IntermediaryRelationJoin) {
					IntermediaryRelationJoin intermediaryRelationJoin = (IntermediaryRelationJoin) relationJoin;
					tables.add((Table<?>) intermediaryRelationJoin.getLeftKey().getTable());
					tables.add((Table<?>) intermediaryRelationJoin.getRightKey().getTable());
					tables.add(intermediaryRelationJoin.getJoinTable());
				}
				if (relation instanceof EntityRelation) {
					relationStack.add(((EntityRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedOneToManyRelation) {
					relationStack.add(((ResolvedOneToManyRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedManyToManyRelation) {
					relationStack.add(((ResolvedManyToManyRelation) relation).getTargetEntity());
				} else if (relation instanceof ResolvedMapRelation) {
					ResolvedMapRelation mapRelation = (ResolvedMapRelation) relation;
					if (mapRelation.getKeyEntityDefinition() != null) {
						relationStack.add(mapRelation.getKeyEntityDefinition().getEntity());
					}
					if (mapRelation.getValueEntityDefinition() != null) {
						relationStack.add(mapRelation.getValueEntityDefinition().getEntity());
					}
				}
			});
			AncestorJoin<?, ?, ?, ?> parent = entity.getParent();
			if (parent != null) {
				relationStack.add(parent.getAncestor());
			}
		}
		return tables;
	}
	
	public static class AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityWriteExecutor<SRC, SRCID> relationOwnerPersister;
		private final String parentJoinPoint;
		private final ReadWritePropertyAccessPoint<SRC, TRGT> accessor;
		
		public AssemblyPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                     EntityWriteExecutor<SRC, SRCID> relationOwnerPersister,
		                     String parentJoinPoint,
		                     ReadWritePropertyAccessPoint<SRC, TRGT> accessor) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.accessor = accessor;
		}
		
		public EntityWriteExecutor<SRC, SRCID> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public String getParentJoinPoint() {
			return parentJoinPoint;
		}
		
		public AbstractEntity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
		
		public ReadWritePropertyAccessPoint<SRC, TRGT> getAccessor() {
			return accessor;
		}
	}
	
	public static class AssemblyPoint2<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityReader<SRC, SRCID, ?> relationOwnerPersister;
		private final String parentJoinPoint;
		private final PropertyAccessor<SRC, TRGT> accessor;
		
		public AssemblyPoint2(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                      EntityReader<SRC, SRCID, ?> relationOwnerPersister,
		                      String parentJoinPoint,
		                      PropertyAccessor<SRC, TRGT> accessor) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.accessor = accessor;
		}
		
		public EntityReader<SRC, SRCID, ?> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public String getParentJoinPoint() {
			return parentJoinPoint;
		}
		
		public AbstractEntity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
		
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
	}
	
	private static class DelegatingConfiguredPersister<C, I> implements ConfiguredPersister<C, I> {
		
		private final DelegatingReadWriteEntityExecutor<C, I> delegate;
		private final Set<Table<?>> tables;
		
		public DelegatingConfiguredPersister(DelegatingReadWriteEntityExecutor<C, I> delegate, Set<? extends Table<?>> tables) {
			this.delegate = delegate;
			this.tables = new HashSet<>(tables);
		}
		
		@Override
		public Collection<Table<?>> giveImpliedTables() {
			return tables;
		}
		
		@Override
		public PersisterListenerCollection<C, I> getPersisterListener() {
			return null;
		}
		
		@Override
		public Set<C> select(Iterable<I> ids) {
			return delegate.select(ids);
		}
		
		@Override
		public RelationalEntityPersister.ExecutableEntityQueryCriteria<C, ?> selectWhere() {
			return delegate.selectWhere();
		}
		
		@Override
		public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
			return delegate.selectProjectionWhere(selectAdapter);
		}
		
		@Override
		public void addSelectListener(SelectListener<? extends C, I> selectListener) {
			delegate.addSelectListener(selectListener);
		}
		
		@Override
		public void persist(Iterable<? extends C> entities) {
			delegate.persist(entities);
		}
		
		@Override
		public <T extends Table<T>> EntityMapping<C, I, T> getMapping() {
			return delegate.getMapping();
		}
		
		@Override
		public Set<C> selectAll() {
			return delegate.selectAll();
		}
		
		@Override
		public boolean isNew(C entity) {
			return delegate.isNew(entity);
		}
		
		@Override
		public I getId(C entity) {
			return delegate.getId(entity);
		}
		
		@Override
		public Class<C> getClassToPersist() {
			return delegate.getClassToPersist();
		}
		
		@Override
		public void delete(Iterable<? extends C> entities) {
			delegate.delete(entities);
		}
		
		@Override
		public void deleteById(Iterable<? extends C> entities) {
			delegate.deleteById(entities);
		}
		
		@Override
		public void insert(Iterable<? extends C> entities) {
			delegate.insert(entities);
		}
		
		@Override
		public void updateById(Iterable<? extends C> entities) {
			delegate.updateById(entities);
		}
		
		@Override
		public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
			delegate.update(differencesIterable, allColumnsStatement);
		}
		
		@Override
		public void addPersistListener(PersistListener<? extends C> persistListener) {
			delegate.addPersistListener(persistListener);
		}
		
		@Override
		public void addInsertListener(InsertListener<? extends C> insertListener) {
			delegate.addInsertListener(insertListener);
		}
		
		@Override
		public void addUpdateListener(UpdateListener<? extends C> updateListener) {
			delegate.addUpdateListener(updateListener);
		}
		
		@Override
		public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
			delegate.addUpdateByIdListener(updateByIdListener);
		}
		
		@Override
		public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
			delegate.addDeleteListener(deleteListener);
		}
		
		@Override
		public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
			delegate.addDeleteByIdListener(deleteListener);
		}
		
		@Override
		public void persist(C entity) {
			delegate.persist(entity);
		}
		
		@Override
		public void insert(C entity) {
			delegate.insert(entity);
		}
		
		@Override
		public void update(C modified, C unmodified, boolean allColumnsStatement) {
			delegate.update(modified, unmodified, allColumnsStatement);
		}
		
		@Override
		public void update(C entity) {
			delegate.update(entity);
		}
		
		@Override
		public void update(C entity, boolean allColumnsStatement) {
			delegate.update(entity, allColumnsStatement);
		}
		
		@Override
		public void update(Iterable<C> entities) {
			delegate.update(entities);
		}
		
		@Experimental
		@Override
		public void update(I id, Consumer<C> entityConsumer) {
			delegate.update(id, entityConsumer);
		}
		
		@Experimental
		@Override
		public void update(Iterable<I> ids, Consumer<C> entityConsumer) {
			delegate.update(ids, entityConsumer);
		}
		
		@Override
		public void delete(C entity) {
			delegate.delete(entity);
		}
		
		@Override
		public void deleteById(C entity) {
			delegate.deleteById(entity);
		}
		
		@Override
		public C select(I id) {
			return delegate.select(id);
		}
		
		@Override
		public Set<C> select(I... ids) {
			return delegate.select(ids);
		}
		
		@Override
		public void updateById(C entity) {
			delegate.updateById(entity);
		}
		
		@Override
		public void persist(C... entities) {
			delegate.persist(entities);
		}
		
		public static <C1, I> PersistExecutor<C1> forPersister(ConfiguredPersister<C1, I> persister) {
			return PersistExecutor.forPersister(persister);
		}
		
		public static <C1, I> PersistExecutor<C1> forPersister(EntityWriteExecutor<C1, I> writer, EntityReadExecutor<C1, I> reader) {
			return PersistExecutor.forPersister(writer, reader);
		}
		
		@Override
		public <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(getter, operator);
		}
		
		@Override
		public <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(setter, operator);
		}
		
		@Override
		public <O, A> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(getter1, getter2, operator);
		}
		
		@Override
		public <O> ExecutableEntityQuery<C, ?> selectWhere(List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(accessorChain, operator);
		}
		
		@Override
		public <O> ExecutableEntityQuery<C, ?> selectWhere(AccessorChain<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(accessorChain, operator);
		}
		
		@Override
		public <O> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
			return delegate.selectWhere(accessorChain, operator);
		}
		
		@Override
		public <O, S extends Collection<O>, NEXT> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1, SerializablePropertyAccessor<O, NEXT> accessor2, ConditionalOperator<NEXT, ?> operator) {
			return delegate.selectWhere(accessor1, accessor2, operator);
		}
		
		@Override
		public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, getter, operator);
		}
		
		@Override
		public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, setter, operator);
		}
		
		@Override
		public <O, A> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, getter1, getter2, operator);
		}
		
		@Override
		public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, accessorChain, operator);
		}
		
		@Override
		public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, EntityCriteria.CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, accessorChain, operator);
		}
		
		@Override
		public <O, S extends Collection<O>, NEXT> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1, SerializablePropertyAccessor<O, NEXT> accessor2, ConditionalOperator<O, ?> operator) {
			return delegate.selectProjectionWhere(selectAdapter, accessor1, accessor2, operator);
		}
	}
}
