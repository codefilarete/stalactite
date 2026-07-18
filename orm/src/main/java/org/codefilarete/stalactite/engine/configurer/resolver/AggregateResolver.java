package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
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
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateResolver {
	
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
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
		this.oneToOneAppender = new AggregateOneToOneAppender(skeletonAggregateResolver);
		this.oneToManyAppender = new AggregateOneToManyAppender(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToManyAppender = new AggregateManyToManyAppender(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.manyToOneAppender = new AggregateManyToOneAppender(skeletonAggregateResolver);
		this.elementCollectionAppender = new AggregateElementCollectionAppender(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.mapAppender = new AggregateMapAppender(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		
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
	
	<C, I> ConfiguredRelationalPersister<C, I> build(AbstractEntity<C, I, ?> rootEntity) {
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
			ConfiguredRelationalPersister<C, I> result = buildPersister(rootEntity);
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
	ConfiguredRelationalPersister<C, I> buildPersister(AbstractEntity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		EntityWriteExecutor<C, I> result = null;
		if (rootEntity instanceof Entity) {
			result = skeletonAggregateResolver.buildPersister((Entity<C, I, T>) rootEntity);
		} else {
			
		}
		
		appendRelations(rootEntity, result);
		
		return result;
	}
	
	
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendRelations(AbstractEntity<SRC, SRCID, LEFTTABLE> rootEntity, EntityWriteExecutor<SRC, SRCID> aggregatePersister) {
		
		// Iterating over all the one-to-many relations of the tree (starting from given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some AssemblyPoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<AssemblyPoint<?, ?, ?, ?>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new AssemblyPoint<>(rootEntity, aggregatePersister, ROOT_JOIN_NAME, null));
		
		Queue<AssemblyPoint<?, ?, ?, ?>> relationStack2 = new ArrayDeque<>();
		
		while (!relationStack.isEmpty()) {
			AssemblyPoint<?, ?, ?, ?> assemblyPawn = relationStack.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							Holder<EntityWriteExecutor<TRGT, Object>> targetPersisterHolder = new Holder<>();
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							// TODO: enhance by returning the built EntityPersister, not by giving it to the Holder
							oneToOneResolver.resolve(
									localRelation,
									aggregatePersister,
									targetPersisterHolder::set);
							AssemblyPoint assemblyPoint = oneToOneAppender.append(
									localRelation,
									targetPersisterHolder.get(),
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							relationStack.add(assemblyPoint);
							relationStack2.add(assemblyPoint);
						}
						if (relationPawn instanceof ResolvedOneToManyRelation) {
							AssemblyPoint<?, ?, ?, ?> assemblyPoint = oneToManyAppender.append(
									aggregatePersister,
									(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
							relationStack.add(assemblyPoint);
						}
						if (relationPawn instanceof ResolvedManyToManyRelation) {
							AssemblyPoint<?, ?, ?, ?> assemblyPoint = manyToManyAppender.append(
									aggregatePersister,
									(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
							relationStack.add(assemblyPoint);
						}
						if (relationPawn instanceof ResolvedManyToOneRelation) {
							AssemblyPoint<?, ?, ?, ?> assemblyPoint = manyToOneAppender.append(
									aggregatePersister,
									(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
							relationStack.add(assemblyPoint);
						}
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							elementCollectionAppender.append(
									aggregatePersister,
									(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							mapAppender.append(
									aggregatePersister,
									(ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
						}
					});
		}
		
		while (!relationStack2.isEmpty()) {
			AssemblyPoint<?, ?, ?, ?> assemblyPawn = relationStack2.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							Holder<EntityWriteExecutor<TRGT, Object>> targetPersisterHolder = new Holder<>();
							ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> localRelation = (ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn;
							AssemblyPoint assemblyPoint = oneToOneAppender.append(
									localRelation,
									targetPersisterHolder.get(),
									assemblyPawn.getParentJoinPoint(),
									localRelation.getAccessor(),
									aggregatePersister.getEntityJoinTree());
							relationStack2.add(assemblyPoint);
						}
//						if (relationPawn instanceof ResolvedOneToManyRelation) {
//							AssemblyPoint<?, ?, ?, ?> assemblyPoint = oneToManyAppender.append(
//									aggregatePersister,
//									(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
//									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
//							relationStack.add(assemblyPoint);
//						}
//						if (relationPawn instanceof ResolvedManyToManyRelation) {
//							AssemblyPoint<?, ?, ?, ?> assemblyPoint = manyToManyAppender.append(
//									aggregatePersister,
//									(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
//									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
//							relationStack.add(assemblyPoint);
//						}
//						if (relationPawn instanceof ResolvedManyToOneRelation) {
//							AssemblyPoint<?, ?, ?, ?> assemblyPoint = manyToOneAppender.append(
//									aggregatePersister,
//									(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE>) relationPawn,
//									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
//							relationStack.add(assemblyPoint);
//						}
//						if (relationPawn instanceof ResolvedElementCollectionRelation) {
//							elementCollectionAppender.append(
//									aggregatePersister,
//									(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn,
//									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
//						}
//						if (relationPawn instanceof ResolvedMapRelation) {
//							mapAppender.append(
//									aggregatePersister,
//									(ResolvedMapRelation<SRC, SRCID, Object, Object, Object, Object, Map<Object, Object>, LEFTTABLE, RIGHTTABLE, ?, ?>) relationPawn,
//									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
//						}
					});
		}
	}
	
	public static class AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final EntityWriteExecutor<SRC, SRCID> relationOwnerPersister;
		private final String parentJoinPoint;
		private final PropertyAccessor<SRC, TRGT> accessor;
		
		public AssemblyPoint(AbstractEntity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                     EntityWriteExecutor<SRC, SRCID> relationOwnerPersister,
		                     String parentJoinPoint,
		                     PropertyAccessor<SRC, TRGT> accessor) {
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
		
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
	}
}
