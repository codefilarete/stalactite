package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.ArrayDeque;
import java.util.Queue;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class AggregateOneToOneAppender {
	
	private final OneToOneResolver oneToOneResolver;
	
	public AggregateOneToOneAppender(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.oneToOneResolver = new OneToOneResolver(skeletonAggregateResolver);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void append(Entity<SRC, SRCID, LEFTTABLE> rootEntity, ConfiguredRelationalPersister<SRC, SRCID> rootPersister) {
		
		// Iterating over all the one-to-one relations of the tree (starting from given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some AssemblyPoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new AssemblyPoint<>(rootEntity, rootPersister, EntityJoinTree.ROOT_JOIN_NAME, null));
		
		while (!relationStack.isEmpty()) {
			AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn = relationStack.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations().stream()
					.filter(ResolvedOneToOneRelation.class::isInstance)
					.map(ResolvedOneToOneRelation.class::cast)
					.forEach(relationPawn -> {
								ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation = relationPawn;
								oneToOneResolver.resolve(
										relation,
										assemblyPawn.getRelationOwnerPersister(),
										targetPersister -> {
											PropertyAccessor<SRC, TRGT> accessor;
											if (assemblyPawn.getParentJoinPoint().equals(EntityJoinTree.ROOT_JOIN_NAME)) {
												// this is the very first step (see stack seed) which is the root entity, no relation accessor shifting here
												accessor = relation.getAccessor();
											} else {
												// we need to shift the relation accessor by the parent accessor
												AccessorChain<SRC, TRGT> shifter = new AccessorChain<>(assemblyPawn.getAccessor(), relation.getAccessor());
												shifter.setNullValueHandler(AccessorChain.RETURN_NULL);
												accessor = shifter;
											}
											
											// we join the relation onto the aggregate root to build the whole select tree
											String joinName = targetPersister.joinAsOne(
													assemblyPawn.getParentJoinPoint(),
													rootPersister,
													accessor,
													relation.getJoin().getLeftKey(),
													relation.getJoin().getRightKey(),
													null,
													relation.getRelationFixer(),
													true,
													false);
											
											// Preparing for next iteration
											// Note that we can't set the correct generics types to the AssemblyPoint instance
											// because we go a step further in the relation by shifting the types from SRC to TRGT 
											relationStack.add(new AssemblyPoint(relation.getTargetEntity(), targetPersister, joinName, accessor));
										});
							}
					);
		}
	}
	
	private static class AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final ConfiguredRelationalPersister<SRC, SRCID> relationOwnerPersister;
		private final String parentJoinPoint;
		private final PropertyAccessor<SRC, TRGT> accessor;
		
		private AssemblyPoint(Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
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
