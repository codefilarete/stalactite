package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collections;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint2;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToOneRelation}.
 * Write cascades are delegated to {@link ManyToOneResolver}.
 *
 * @author Guillaume Mary
 */
public class AggregateManyToOneAppender {
	
	/**
	 * Appends the given many-to-one relation to the aggregate persister by:
	 * - Delegating write-cascade setup to {@link ManyToOneResolver}.
	 * - Adding the necessary join segments to the root persister's join tree.
	 *
	 * @return an {@link AssemblyPoint2} for the target entity, ready to be pushed onto the assembly queue
	 * so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	AssemblyPoint2<TRGT, TRGTID, ?, RIGHTTABLE> append(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                                   EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
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
				new EntityMappingAdapter<>(targetPersister.getMapping()),
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
