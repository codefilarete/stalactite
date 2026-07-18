package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collections;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateOneToOneAppender {
	
	private final OneToOneResolver oneToOneResolver;
	
	public AggregateOneToOneAppender(SkeletonAggregateResolver skeletonAggregateResolver) {
		this.oneToOneResolver = new OneToOneResolver(skeletonAggregateResolver);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	AssemblyPoint append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                     ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relationPawn,
	                     AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn) {
		
		ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation = relationPawn;
		Holder<AssemblyPoint> resultHolder = new Holder<>();
		oneToOneResolver.resolve(
				relation,
				assemblyPawn.getRelationOwnerPersister(),
				targetPersister -> {
					resultHolder.set(append(relation, targetPersister, assemblyPawn.getParentJoinPoint(), assemblyPawn.getAccessor(), rootPersister.getEntityJoinTree()));
				});
		return resultHolder.get();
	}
	
	public <SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	AssemblyPoint append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                     EntityWriteExecutor<TRGT, Object> targetPersister,
	                     String mountPoint,
	                     PropertyAccessor<SRC, TRGT> targetPropertyAccessor,
	                     EntityJoinTree<SRC, SRCID> aggregateTree) {
		PropertyAccessor<SRC, TRGT> accessor;
		if (mountPoint.equals(EntityJoinTree.ROOT_JOIN_NAME)) {
			// this is the very first step (see stack seed) which is the root entity, no relation accessor shifting here
			accessor = relation.getAccessor();
		} else {
			// we need to shift the relation accessor by the parent accessor
			AccessorChain<SRC, TRGT> shifter = new AccessorChain<>(targetPropertyAccessor, relation.getAccessor());
			shifter.setNullValueHandler(AccessorChain.RETURN_NULL);
			accessor = shifter;
		}
		
		// we join the relation onto the aggregate root to build the whole select tree
		String joinName = aggregateTree.addRelationJoin(
				mountPoint,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
				accessor,
				relation.getJoin().getLeftKey(),
				relation.getJoin().getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),
				Collections.emptySet());
		
		return new AssemblyPoint(relation.getTargetEntity(), targetPersister, joinName, accessor);
	}
}
