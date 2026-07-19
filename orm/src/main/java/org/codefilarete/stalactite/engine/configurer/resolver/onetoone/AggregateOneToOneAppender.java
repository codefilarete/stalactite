package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collections;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint2;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateOneToOneAppender {
	
	/**
	 *
	 * @param relation
	 * @param targetPersister
	 * @param mountPoint
	 * @param targetPropertyAccessor
	 * @param aggregateTree
	 * @param <SRC>
	 * @param <SRCID>
	 * @param <TRGT>
	 * @param <TRGTID>
	 * @param <LEFTTABLE>
	 * @param <RIGHTTABLE>
	 * @param <JOINID> either SRCID or TRGTID, depending on the relation owner
	 * @return
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                                                   EntityReader<TRGT, TRGTID, ?> targetPersister,
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
				new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
				accessor,
				relation.getJoin().getLeftKey(),
				relation.getJoin().getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),
				Collections.emptySet());
		
		return new AssemblyPoint2(relation.getTargetEntity(), targetPersister, joinName, accessor);
	}
}
