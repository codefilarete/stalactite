package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.JoinLink;
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
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                                                        ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                        EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                                        String mountPoint,
	                                                        EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// TODO: do the same for Many-to-one
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		if (relation.isFetchSeparately()) {
			ThreadLocal<RelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
			Function<ColumnedRow, TRGTID> idMapping;
			if (relation.isOwnedByTarget()) {
				// We build a function capable of building the identifier from the association table columns, because,
				// if we give the targetPersister identifier assembler then the runtime fails : identifier takes its values
				// from the target table columns which are missins in the join : the join only contains the right
				// table columns and the association ones (that's separate-load principle)
				KeyMapping<RIGHTTABLE, LEFTTABLE, TRGTID> targetPkToRightKey = new KeyMapping<>(targetPersister.getMapping().getTargetTable().getPrimaryKey(), (Key<LEFTTABLE, TRGTID>) join.getLeftKey());
				KeepOrderMap<JoinLink<RIGHTTABLE, ?>, JoinLink<LEFTTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
				idMapping = columnedRow -> targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(new ColumnedRow() {
					@Override
					public <E> E get(Selectable<E> pkColumn) {
						return (E) columnedRow.get(targetPkToAssociationTableKey.get(pkColumn));
					}
				});
			} else {
				idMapping = targetPersister.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
			}
			
			// here is the logic below :
			// - we collect the SRC-Index-TRGTID on the association join: see FirstPhaseIndexedRelationLoader usage hereafter
			// - then we trigger the target entities collect on the afterSelect of the source
			// - just after, we can apply the relation
			aggregateTree.addMergeJoin(mountPoint,
					new org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader<>(idMapping, (Set) targetPersister.getMapping().getTargetTable().getPrimaryKey().getColumns(), current2PhasesLoadContext),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			
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
					Set<TRGT> targets = targetPersister.select(trgtids);
					Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetPersister.getMapping()::getId));
					
					// we sow the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						Set<TRGTID> targetIdPerIndex = targetIdPerSource.get(src);
						if (targetIdPerIndex != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
							TRGTID trgtId = Iterables.first(targetIdPerIndex);
							TRGT trgt = targetPerId.get(trgtId);
							relation.getAccessor().set(src, trgt);
						}
					});
					
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
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
		} else {
			// we join the relation onto the aggregate root to build the whole select tree
			String joinName = aggregateTree.addRelationJoin(
					mountPoint,
					new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
					relation.getAccessor(),
					join.getLeftKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),
					Collections.emptySet());
			
			result = new GraftPoint<>(relation.getTargetEntity(), targetPersister, joinName, aggregateTree);
		}
		return result;
	}
}
