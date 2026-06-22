package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collections;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToOneRelation}.
 * Write cascades are delegated to {@link ManyToOneResolver}.
 *
 * @author Guillaume Mary
 */
public class AggregateManyToOneAppender {
	
	private final ManyToOneResolver manyToOneResolver;
	
	public AggregateManyToOneAppender(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.manyToOneResolver = new ManyToOneResolver(skeletonAggregateResolver);
	}
	
	/**
	 * Appends the given many-to-one relation to the aggregate persister by:
	 * - Delegating write-cascade setup to {@link ManyToOneResolver}.
	 * - Adding the necessary join segments to the root persister's join tree.
	 *
	 * @return an {@link AssemblyPoint} for the target entity, ready to be pushed onto the assembly queue
	 *         so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	AssemblyPoint<?, ?, ?, ?> append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                 ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                 AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn) {
		
		Holder<AssemblyPoint> resultHolder = new Holder<>();
		manyToOneResolver.resolve(
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
					
					String joinName = rootPersister.getEntityJoinTree().addRelationJoin(
							assemblyPawn.getParentJoinPoint(),
							new EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
							accessor,
							relation.getJoin().getLeftKey(),
							relation.getJoin().getRightKey(),
							null,
							OUTER,
							relation.getRelationFixer(),
							Collections.emptySet());
					
					resultHolder.set(new AssemblyPoint(relation.getTargetEntity(), targetPersister, joinName, accessor));
				});
		
		return resultHolder.get();
	}
}
