package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.RelationIds;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyWithMappedAssociationAppender {
	
	private final PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		GraftPoint result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) relation.getJoin();
		if (relation.isFetchedSeparately()) {
			result = appendWithSeparateFetch(relation, sourceReader, targetReader, mountPoint, aggregateTree, join);
		} else {
			
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, TRGTID> polymorphism = ((PolymorphicEntity<TRGT, TRGTID, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, TRGTID> graftableRelation = GraftableRelation.forOneToManyRelationOwnedByTarget(relation);
				result = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetReader,
						graftableRelation,
						mountPoint);
			} else {
				Set<Column<RIGHTTABLE, ?>> columnsToSelect;
				Function<ColumnedRow, Object> duplicateIdentifierProvider;
				if (relation.isOrdered()) {
					columnsToSelect = new HashSet<>(targetReader.getMapping().getTargetTable().getPrimaryKey().getColumns());
					columnsToSelect.add(relation.getIndexingMappedColumn());
					duplicateIdentifierProvider = (columnedRow) -> {
						TRGTID identifier = targetReader.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
						Integer targetEntityIndex = columnedRow.get(relation.getIndexingMappedColumn());
						return identifier + "-" + targetEntityIndex;
					};
				} else {
					columnsToSelect = Collections.emptySet();
					duplicateIdentifierProvider = targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
				}
				
				String manyJoinName = aggregateTree.addRelationJoin(
						mountPoint,
						new EntityMappingAdapter<>(targetReader.getMapping()),
						relation.getAccessor(),
						join.getLeftKey(),
						join.getRightKey(),
						null,
						OUTER,
						relation.getRelationFixer(),
						columnsToSelect,
						duplicateIdentifierProvider);
				result = new GraftPoint(relation.getTargetEntity(), targetReader, manyJoinName, aggregateTree);
			}
		}
		
		SelectListener<TRGT, TRGTID> targetSelectListener = targetReader.getSelectListener();
		sourceReader.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
								.map(Collection::stream)
								.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
		return result;
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID>
	appendWithSeparateFetch(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
							ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
							ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
							String mountPoint,
							EntityJoinTree<SRC, SRCID> aggregateTree,
							DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join) {
		GraftPoint result;
		ThreadLocalRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalRelationStorage<>();
		if (relation.getTargetEntity() instanceof PolymorphicEntity) {
			EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
			
			polymorphicSkeletonAppender.appendForSeparateLoad(
					aggregateTree,
					polymorphism,
					targetReader,
					mountPoint,
					current2PhasesLoadContext,
					join.getKeyMapping());
			// adding second phase loader
			sourceReader.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					current2PhasesLoadContext.init();
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Map<SRC, Set<TRGTID>> targetIdPerSource = current2PhasesLoadContext.getStorage().getTargetIdPerSource();
					Set<TRGTID> trgtids = targetIdPerSource.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
					if (!trgtids.isEmpty()) {	// we only avoid some extra work if there's nothing to do
						Set<TRGT> targets = targetReader.select(trgtids);
						Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetReader.getMapping()::getId));
						
						// we sew the relations
						result.forEach(src -> {
							// filling the collection
							Set<TRGTID> sourceTargetId = targetIdPerSource.get(src);
							if (sourceTargetId != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
								Set<TRGT> sourceTargets = sourceTargetId.stream().map(targetPerId::get).collect(Collectors.toSet());
								sourceTargets.forEach(target -> relation.getRelationFixer().apply(src, target));
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
					current2PhasesLoadContext.clear();
				}
			});
		} else {
			ThreadLocal<Queue<Set<RelationIds<SRC /* E */, TRGT /* target */, TRGTID /* target identifier */>>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
			aggregateTree.addMergeJoin(mountPoint,
					new FirstPhaseRelationLoader<>(targetReader.getMapping().getIdMapping(), targetReader,
							(ThreadLocal<Queue<Set<RelationIds<Object, TRGT, TRGTID>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			// adding second phase loader
			sourceReader.addSelectListener(new SecondPhaseRelationLoader<>(relation.getRelationFixer(), CURRENT_2PHASES_LOAD_CONTEXT));
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
		}
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, targetReader.getEntityJoinTree());
	}
}
