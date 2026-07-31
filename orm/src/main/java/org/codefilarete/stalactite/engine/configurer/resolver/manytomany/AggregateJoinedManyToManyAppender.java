package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

/**
 * Wires an unordered many-to-many relation onto the aggregate's {@link EntityJoinTree}, so that target entities are
 * loaded by the very same query as the one that loads the aggregate root : two join segments are added, a passive one
 * from the source table to the association table, then a relation one from the association table to the target table.
 *
 * @author Guillaume Mary
 * @see AggregateFetchSeparatelyManyToManyAppender
 */
public class AggregateJoinedManyToManyAppender {
	
	/**
	 * @return the graft point of the target entity onto the aggregate tree, so that the relations of the target entity
	 * 		   are loaded by that very same query
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  PropertyAccessor<SRC, S> accessor,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		
		String associationTableJoinName = aggregateTree.addPassiveJoin(
				mountPoint,
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				Collections.emptySet());
		
		String manyJoinName = aggregateTree.addRelationJoin(
				associationTableJoinName,
				new EntityMappingAdapter<>(targetPersister.getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),   // pre-built fixer handles bidirectionality if configured
				Collections.emptySet(),
				null);
		return new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, aggregateTree);
	}
}
