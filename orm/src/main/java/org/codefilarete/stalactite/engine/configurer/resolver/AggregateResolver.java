package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AggregateMetadataResolver;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.AggregateElementCollectionAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.map.AggregateMapAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.manytomany.AggregateManyToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetomany.AggregateOneToManyAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.onetoone.AggregateOneToOneAppender;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateResolver {
	
	private final AggregateMetadataResolver aggregateMetadataResolver;
	private final PersisterRegistry persisterRegistry;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final AggregateOneToOneAppender oneToOneAppender;
	private final AggregateOneToManyAppender oneToManyAppender;
	private final AggregateManyToManyAppender manyToManyAppender;
	private final AggregateElementCollectionAppender elementCollectionAppender;
	private final AggregateMapAppender mapAppender;
	
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
		this.elementCollectionAppender = new AggregateElementCollectionAppender(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.mapAppender = new AggregateMapAppender(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public <C, I> EntityPersister<C, I> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		Entity<C, I, ?> rootEntity = aggregateMetadataResolver.resolve(rootConfiguration);
		return build(rootEntity);
	}
	
	<C, I> ConfiguredRelationalPersister<C, I> build(Entity<C, I, ?> rootEntity) {
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
	ConfiguredRelationalPersister<C, I> buildPersister(Entity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		ConfiguredRelationalPersister<C, I> result = skeletonAggregateResolver.buildPersister(rootEntity);
		
		appendRelations(rootEntity, result);
		
		return result;
	}
	
	
	
	<SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendRelations(Entity<SRC, SRCID, LEFTTABLE> rootEntity, ConfiguredRelationalPersister<SRC, SRCID> aggregatePersister) {
		
		// Iterating over all the one-to-many relations of the tree (starting from given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some AssemblyPoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<AssemblyPoint<?, ?, ?, ?>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new AssemblyPoint<>(rootEntity, aggregatePersister, ROOT_JOIN_NAME, null));
		
		while (!relationStack.isEmpty()) {
			AssemblyPoint<?, ?, ?, ?> assemblyPawn = relationStack.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations()
					.forEach(relationPawn -> {
						if (relationPawn instanceof ResolvedOneToOneRelation) {
							AssemblyPoint<?, ?, ?, ?> assemblyPoint = oneToOneAppender.append(
									aggregatePersister,
									(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
							relationStack.add(assemblyPoint);
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
						if (relationPawn instanceof ResolvedElementCollectionRelation) {
							elementCollectionAppender.append(
									aggregatePersister,
									(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, RIGHTTABLE, ElementRecord<TRGT, SRCID>>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
						}
						if (relationPawn instanceof ResolvedMapRelation) {
							mapAppender.append(
									aggregatePersister,
									(ResolvedMapRelation<SRC, Object, Object, Map<Object, Object>, SRCID, LEFTTABLE, RIGHTTABLE>) relationPawn,
									(AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>) assemblyPawn);
						}
					});
		}
	}
	
	public static class AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final ConfiguredRelationalPersister<SRC, SRCID> relationOwnerPersister;
		private final String parentJoinPoint;
		private final PropertyAccessor<SRC, TRGT> accessor;
		
		public AssemblyPoint(Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                     ConfiguredRelationalPersister<SRC, SRCID> relationOwnerPersister,
		                     String parentJoinPoint,
		                     PropertyAccessor<SRC, TRGT> accessor) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.accessor = accessor;
		}
		
		public ConfiguredRelationalPersister<SRC, SRCID> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public String getParentJoinPoint() {
			return parentJoinPoint;
		}
		
		public Entity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
		
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
	}
}
