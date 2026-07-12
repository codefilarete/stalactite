package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collections;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
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
							Collections.emptySet()
					);
					
					resultHolder.set(new AssemblyPoint(relation.getTargetEntity(), targetPersister, joinName, accessor));
				});
		return resultHolder.get();
	}
}
