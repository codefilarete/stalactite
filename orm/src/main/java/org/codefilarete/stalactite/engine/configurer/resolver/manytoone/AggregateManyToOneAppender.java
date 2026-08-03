package org.codefilarete.stalactite.engine.configurer.resolver.manytoone;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

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
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> append(ResolvedManyToOneRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                                        ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                        ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                                        String mountPoint,
	                                                        EntityJoinTree<SRC, SRCID> aggregateTree) {
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, TRGTID> join = relation.getJoin();
		if (relation.isFetchSeparately()) {
			ThreadLocal<RelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
			Function<ColumnedRow, TRGTID> idMapping = targetPersister.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
			
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
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, joinName);
		}
		return result;
	}
}
