package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.listener.SelectListener;
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
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                            EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		GraftPoint result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) relation.getJoin();
		if (relation.isFetchSeparately()) {
			ThreadLocal<Queue<Set<RelationIds<SRC /* E */, TRGT /* target */, TRGTID /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
			aggregateTree.addMergeJoin(mountPoint,
					new FirstPhaseRelationLoader<>(targetPersister.getMapping().getIdMapping(), targetPersister,
							(ThreadLocal<Queue<Set<RelationIds<Object, TRGT, TRGTID>>>>) (ThreadLocal) CURRENT_2PHASES_LOAD_CONTEXT),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			// adding second phase loader
			sourcePersister.addSelectListener(new SecondPhaseRelationLoader<>(relation.getRelationFixer(), CURRENT_2PHASES_LOAD_CONTEXT));
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
		} else {
			Set<Column<RIGHTTABLE, ?>> columnsToSelect;
			Function<ColumnedRow, Object> duplicateIdentifierProvider;
			if (relation.isOrdered()) {
				columnsToSelect = new HashSet<>(targetPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
				columnsToSelect.add(relation.getIndexingMappedColumn());
				duplicateIdentifierProvider = (columnedRow) -> {
					TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
					Integer targetEntityIndex = columnedRow.get(relation.getIndexingMappedColumn());
					return identifier + "-" + targetEntityIndex;
				};
			} else {
				columnsToSelect = Collections.emptySet();
				duplicateIdentifierProvider = targetPersister.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
			}
		
			String manyJoinName = aggregateTree.addRelationJoin(
					mountPoint,
					new EntityMappingAdapter<>(targetPersister.getMapping()),
					relation.getAccessor(),
					join.getLeftKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),
					columnsToSelect,
					duplicateIdentifierProvider);
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, aggregateTree);
		}
		
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
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
}
