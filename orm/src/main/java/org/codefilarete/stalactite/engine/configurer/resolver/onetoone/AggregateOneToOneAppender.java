package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

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
		if (relation.isFetchSeparately()) {
			ThreadLocal<RelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
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
			} else {
				aggregateTree.addMergeJoin(mountPoint,
						new org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader<>(idMapping, (Set) targetReader.getMapping().getTargetTable().getPrimaryKey().getColumns(), current2PhasesLoadContext),
						join.getLeftKey(),
						join.getRightKey(),
						OUTER);
			}
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					current2PhasesLoadContext.set(new RelationStorage<>());
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Map<SRC, Set<TRGTID>> targetIdPerSource = current2PhasesLoadContext.get().getTargetIdPerSource();
					Set<TRGTID> trgtids = targetIdPerSource.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
					if (!trgtids.isEmpty()) {	// we only avoid some extra work if there's nothing to do
						Set<TRGT> targets = targetReader.select(trgtids);
						Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetReader.getMapping()::getId));
						
						// we sew the relations
						result.forEach(src -> {
							// filling final collection with a sorted collection
							Set<TRGTID> targetIdPerIndex = targetIdPerSource.get(src);
							if (targetIdPerIndex != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
								TRGTID trgtId = Iterables.first(targetIdPerIndex);
								TRGT trgt = targetPerId.get(trgtId);
								relation.getRelationFixer().apply(src, trgt);
							}
						});
					}
					
					clearContext();
				}
				
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					clearContext();
				}
				
				private void clearContext() {
					current2PhasesLoadContext.remove();
				}
			});
			
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
			result = new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, targetReader.getEntityJoinTree());
		} else {
			// we join the relation onto the aggregate root to build the whole select tree
			
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				result = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetReader,
						relation,
						mountPoint);
			} else {
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
}
