package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collections;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
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
	 * @return an {@link GraftPoint} for the target entity, ready to be pushed onto the assembly queue
	 * so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE> append(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                            EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		String joinName = aggregateTree.addRelationJoin(
				mountPoint,
				new EntityMappingAdapter<>(targetPersister.getMapping()),
				relation.getAccessor(),
				relation.getJoin().getLeftKey(),
				relation.getJoin().getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),
				Collections.emptySet());
		return new GraftPoint(relation.getTargetEntity(), targetPersister, joinName);
	}
}
