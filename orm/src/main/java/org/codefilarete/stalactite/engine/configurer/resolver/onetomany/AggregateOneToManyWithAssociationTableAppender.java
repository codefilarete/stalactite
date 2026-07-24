package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.RelationIds;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyWithAssociationTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                            EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		return appendAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint);
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                             EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                             ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                             PropertyAccessor<SRC, S> accessor,
	                             EntityJoinTree<SRC, SRCID> entityJoinTree,
	                             String mountPoint) {
		Function<ColumnedRow, Object> duplicateIdentifierProvider = null;
		
		// we join on the association table
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		String associationTableJoinName = entityJoinTree.addPassiveJoin(
				mountPoint,
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				Collections.emptySet());
		if (relation.isFetchSeparately()) {
			ThreadLocal<Queue<Set<RelationIds<SRC /* E */, TRGT /* target */, TRGTID /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
			entityJoinTree.addMergeJoin(mountPoint,
					new FirstPhaseRelationLoader<>(targetPersister.getMapping().getIdMapping(), targetPersister,
							(ThreadLocal<Queue<Set<RelationIds<Object, TRGT, TRGTID>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
					join.getRightAssociationKey(),
					join.getRightKey(),
					OUTER);
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(relation.getRelationFixer(), CURRENT_2PHASES_LOAD_CONTEXT));
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
			return new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
		} else {
			String manyJoinName = entityJoinTree.addRelationJoin(
					associationTableJoinName,
					new EntityMappingAdapter<>(targetPersister.getMapping()),
					accessor,
					join.getRightAssociationKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),
					Collections.emptySet(),
					duplicateIdentifierProvider);
			return new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, entityJoinTree);
		}
	}
}
