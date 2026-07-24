package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
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
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;

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
		this.manyToManyAppender = new AggregateManyToManyAppender();
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
		// Bread-first principle shouldn't be important because we maintain some CascadePoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<CascadePoint<?, ?, ?>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new CascadePoint<>(rootEntity, aggregatePersister));
		
		Map<MappingJoin<LEFTTABLE, RIGHTTABLE, ?>, Object> result = new HashMap<>();
		
		while (!relationStack.isEmpty()) {
			CascadePoint<?, ?, ?> assemblyPawn = relationStack.poll();
			EntityWriteExecutor<SRC, SRCID> relationOwnerPersister = (EntityWriteExecutor<SRC, SRCID>) assemblyPawn.getRelationOwnerPersister();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector,
									localRelation.getAccessor());
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							oneToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToManyResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							CreatedPersisterCollector<TRGT, TRGTID> createdPersisterCollector = new CreatedPersisterCollector<>();
							manyToOneResolver.resolve(
									localRelation,
									relationOwnerPersister,
									createdPersisterCollector);
							result.put(localRelation, createdPersisterCollector);
							relationStack.add(new CascadePoint(localRelation.getTargetEntity(), createdPersisterCollector.getPersister()));
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
								relationStack.add(new CascadePoint(localRelation.getKeyEntityDefinition().getEntity(), mapCreatedPersisterCollector.getKeyPersisterCollector().getPersister()));
							}
							if (localRelation.getValueEntityDefinition() != null) {
								relationStack.add(new CascadePoint(localRelation.getValueEntityDefinition().getEntity(), mapCreatedPersisterCollector.getValuePersisterCollector().getPersister()));
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
		
		Queue<GraftPoint<?, ?, ?, ?, ?>> relationStack = new ArrayDeque<>();
		relationStack.add(new GraftPoint<>(rootEntity, aggregatePersister, ROOT_JOIN_NAME, aggregatePersister.getEntityJoinTree()));
		new SkeletonAggregateAppender()
				.appendInheritance((CreatedPersisterCollector<SRC, SRCID>) createdPersisters.get(null), aggregatePersister.getEntityJoinTree());
		
		while (!relationStack.isEmpty()) {
			GraftPoint<SRC, SRCID, LEFTTABLE, SRC, SRCID> assemblyPawn = (GraftPoint<SRC, SRCID, LEFTTABLE, SRC, SRCID>) relationStack.poll();
			EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister = (EntityReader<SRC, SRCID, LEFTTABLE>) assemblyPawn.getRelationOwnerPersister();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = oneToOneAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = oneToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = manyToManyAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> localRelation = (ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn;
							EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister = new EntityReader<>(
									localCreatedPersistor.getPersister().<RIGHTTABLE>getMapping(), persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
							GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> graftPoint = manyToOneAppender.append(
									localRelation,
									sourcePersister,
									targetPersister,
									assemblyPawn.getParentJoinPoint(),
									assemblyPawn.getAggregateTree());
							new SkeletonAggregateAppender()
									.appendInheritance(localCreatedPersistor, aggregatePersister.getEntityJoinTree());
							relationStack.add(graftPoint);
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							CreatedPersisterCollector<TRGT, TRGTID> localCreatedPersistor = (CreatedPersisterCollector<TRGT, TRGTID>) createdPersisters.get(relationPawn);
							EntityReadWriteExecutor<TRGT, TRGTID> persister = localCreatedPersistor.getPersister();
							elementCollectionAppender.append(
									(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn,
									assemblyPawn.getAggregateTree(),
									(ElementCollectionResolver.ElementRecordPersister<TRGT, SRCID, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) persister,
									assemblyPawn.getParentJoinPoint()
							);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							MapCreatedPersisterCollector mapCreatedPersisterCollector = (MapCreatedPersisterCollector) createdPersisters.get(relationPawn);
							ResolvedMapRelation localRelation = (ResolvedMapRelation) relationPawn;
							Duo<GraftPoint, GraftPoint> keyValueGraftPoints = graftMapRelation(localRelation, assemblyPawn.getAggregateTree(), assemblyPawn.getParentJoinPoint(), sourcePersister, mapCreatedPersisterCollector);
							
							if (keyValueGraftPoints.getLeft() != null) {
								relationStack.add(keyValueGraftPoints.getLeft());
							}
							if (keyValueGraftPoints.getRight() != null) {
								relationStack.add(keyValueGraftPoints.getRight());
							}
						}
					});
		}
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint, GraftPoint> graftMapRelation(ResolvedMapRelation relation,
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
		
		return mapAppender.append(
				typedRelation,
				aggregateTree,
				relationOwnerPersister,
				mountPoint,
				typedMapCreatedPersisterCollector.getKeyValueRecordPersister(),
				keyEntityReader,
				valueEntityReader,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionProvider()
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
	
	public static class CascadePoint<SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityWriteExecutor<SRC, SRCID> relationOwnerPersister;
		
		public CascadePoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                    EntityWriteExecutor<SRC, SRCID> relationOwnerPersister) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
		}
		
		public EntityWriteExecutor<SRC, SRCID> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public AbstractEntity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
	}
	
	public static class GraftPoint<SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>, ROOT, ROOTID> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityReader<SRC, SRCID, ?> relationOwnerPersister;
		private final String parentJoinPoint;
		private EntityJoinTree<ROOT, ROOTID> aggregateTree;
		
		public GraftPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                  EntityReader<SRC, SRCID, ?> relationOwnerPersister,
		                  String parentJoinPoint) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
		}
		
		public GraftPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                  EntityReader<SRC, SRCID, ?> relationOwnerPersister,
		                  String parentJoinPoint,
		                  EntityJoinTree<ROOT, ROOTID> aggregateTree) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.aggregateTree = aggregateTree;
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
		
		public EntityJoinTree<ROOT, ROOTID> getAggregateTree() {
			return aggregateTree;
		}
	}
}
