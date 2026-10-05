package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collections;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToOneRelation}.
 * Write cascades are delegated to {@link ManyToOneResolver}.
 *
 * @author Guillaume Mary
 */
public class AggregateManyToOneAppender {
	
	private final PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
	
	/**
	 * Appends the given many-to-one relation to the aggregate persister by:
	 * - Delegating write-cascade setup to {@link ManyToOneResolver}.
	 * - Adding the necessary join segments to the root persister's join tree.
	 *
	 * @return an {@link GraftPoint} for the target entity, ready to be pushed onto the assembly queue
	 * so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> append(ResolvedManyToOneRelation<SRC, TRGT, SRCID, LEFTTABLE, RIGHTTABLE> relation,
	                                                        ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
	                                                        ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
	                                                        String mountPoint,
	                                                        EntityJoinTree<SRC, SRCID> aggregateTree) {
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = relation.getJoin();
		if (relation.isFetchedSeparately()) {
			result = appendWithSeparateFetch(relation, sourceReader, targetReader, mountPoint, aggregateTree, join);
		} else {
			// we join the relation onto the aggregate root to build the whole select tree
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				// Note that there's no difference between relation being owned by source or target, because the join
				// is defined by the DirectRelationJoin object, which is built accordingly to the relation owner
				GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, SRCID> graftableRelation = new GraftableRelation<>(
						relation.getJoin().getLeftKey(),
						relation.getJoin().getRightKey(),
						relation.getAccessor(),
						(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
						relation.getTargetEntity());
				result = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetReader,
						graftableRelation,
						mountPoint);
			} else {
				String joinName = aggregateTree.addRelationJoin(
						mountPoint,
						new EntityMappingAdapter<>(targetReader.getMapping()),
						relation.getAccessor(),
						relation.getJoin().getLeftKey(),
						relation.getJoin().getRightKey(),
						null,
						OUTER,
						relation.getRelationFixer(),
						Collections.emptySet());
				result = new GraftPoint(relation.getTargetEntity(), targetReader, joinName);
			}
		}
		return result;
	}
	
	private <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID>
	appendWithSeparateFetch(ResolvedManyToOneRelation<SRC, TRGT, SRCID, LEFTTABLE, RIGHTTABLE> relation,
							ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
							ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
							String mountPoint,
							EntityJoinTree<SRC, SRCID> aggregateTree,
							DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join) {
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		ThreadLocalRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalRelationStorage<>();
		
		// here is the logic below :
		// - we collect the SRC-Index-TRGTID on the association join: see FirstPhaseIndexedRelationLoader usage hereafter
		// - then we trigger the target entities collect on the afterSelect of the source
		// - just after, we can apply the relation
		if (relation.getTargetEntity() instanceof PolymorphicEntity) {
			EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
			polymorphicSkeletonAppender.appendForSeparateLoad(
					aggregateTree,
					polymorphism,
					targetReader,
					mountPoint,
					current2PhasesLoadContext,
					relation.getJoin().getKeyMapping());
			
			sourceReader.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		} else {
			FirstPhaseRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseRelationLoader<>(
					targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble,
					targetReader.getMapping().getTargetTable().getPrimaryKey().getColumns(),
					current2PhasesLoadContext);
			aggregateTree.addMergeJoin(mountPoint,
					firstPhaseCollector,
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			
			sourceReader.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		}
		
		// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
		// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, targetReader.getEntityJoinTree());
	}
}
