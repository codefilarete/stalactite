package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collections;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
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
	GraftPoint<TRGT, TRGTID, RIGHTTABLE> append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                                            EntityReader<TRGT, TRGTID, ?> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		// we join the relation onto the aggregate root to build the whole select tree
		String joinName = aggregateTree.addRelationJoin(
				mountPoint,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
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
