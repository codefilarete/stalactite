package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collections;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation.forOneToOneRelationOwnedByTarget;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToOneAppender {
	
	private final PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
	
	/**
	 *
	 * @param relation
	 * @param targetReader
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
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
															ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
															ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
															String mountPoint,
															EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		if (relation.isFetchedSeparately()) {
			result = appendWithSeparateFetch(relation, sourcePersister, targetReader, mountPoint, aggregateTree, join);
		} else {
			// we join the relation onto the aggregate root to build the whole select tree
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				// Note that there's no difference between relation being owned by source or target, because the join
				// is defined by the DirectRelationJoin object, which is built accordingly to the relation owner
				GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, JOINID> graftableRelation = forOneToOneRelationOwnedByTarget(relation);
				result = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetReader,
						graftableRelation,
						mountPoint);
			} else {
				// Note that there's no difference between relation being owned by source or target, because the join
				// is defined by the DirectRelationJoin object, which is built accordingly to the relation owner
				String joinName = aggregateTree.addRelationJoin(
						mountPoint,
						new EntityMappingAdapter<>(targetReader.getMapping()),
						relation.getAccessor(),
						join.getLeftKey(),
						join.getRightKey(),
						null,
						OUTER,
						relation.getRelationFixer(),
						Collections.emptySet());
				result = new GraftPoint<>(relation.getTargetEntity(), targetReader, joinName, aggregateTree);
			}
		}
		return result;
	}
	
	private <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID>
	appendWithSeparateFetch(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
							ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
							ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
							String mountPoint,
							EntityJoinTree<SRC, SRCID> aggregateTree,
							DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		ThreadLocalRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalRelationStorage<>();
		Function<ColumnedRow, TRGTID> idMapping;
		if (relation.isOwnedByTarget()) {
			// We build a function capable of building the identifier from the association table columns, because,
			// if we give the targetPersister identifier assembler, then the runtime fails : identifier takes its values
			// from the target table columns which are missing in the join : the join only contains the right
			// table columns and the association ones (that's separate-load principle)
			KeyMapping<RIGHTTABLE, LEFTTABLE, TRGTID> targetPkToRightKey = new KeyMapping<>(targetReader.getMapping().getTargetTable().getPrimaryKey(), (Key<LEFTTABLE, TRGTID>) join.getLeftKey());
			KeepOrderMap<QualifiedSelectable<RIGHTTABLE, ?>, QualifiedSelectable<LEFTTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
			idMapping = columnedRow -> targetReader.getMapping().getIdMapping().getIdentifierAssembler().assemble(new ColumnedRow() {
				@Override
				public <E> E get(Selectable<E> pkColumn) {
					return (E) columnedRow.get(targetPkToAssociationTableKey.get(pkColumn));
				}
			});
		} else {
			idMapping = targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
		}
		
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
			
			sourcePersister.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		} else {
			FirstPhaseRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseRelationLoader<>(idMapping, targetReader.getMapping().getTargetTable().getPrimaryKey().getColumns(), current2PhasesLoadContext);
			aggregateTree.addMergeJoin(mountPoint,
					firstPhaseCollector,
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			
			sourcePersister.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		}
		
		// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
		// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
		result = new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, targetReader.getEntityJoinTree());
		return result;
	}
}
