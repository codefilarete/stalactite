package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseIndexedRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.IndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyWithIndexedMappedAssociationAppender {
	
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
		Column<RIGHTTABLE, Integer> indexingColumn = relation.getIndexingMappedColumn();
		
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetPersisterMapping = targetPersister.getMapping();
		IdentifierAssembler<TRGTID, ?> targetIdentifierAssembler = targetPersisterMapping.getIdMapping().getIdentifierAssembler();
		if (relation.isFetchSeparately()) {
			ThreadLocal<IndexedRelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
			
			// here is the logic below :
			// - we collect the SRC-Index-TRGTID on the association join: see FirstPhaseIndexedRelationLoader usage hereafter
			// - then we trigger the target entities collect on the afterSelect of the source
			// - just after, we can apply the relation
			aggregateTree.addMergeJoin(mountPoint,
					// we give the targetIDentifierAssembler column to ensure it can assemble the id from the ColumnedRow
					new FirstPhaseIndexedRelationLoader<SRC, TRGTID>(targetIdentifierAssembler::assemble, (Set) targetIdentifierAssembler.getColumns(), indexingColumn,
							current2PhasesLoadContext),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					current2PhasesLoadContext.set(new IndexedRelationStorage<>());
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Map<SRC, Map<Integer, TRGTID>> targetIdPerIndexPerSource = current2PhasesLoadContext.get().getTargetIdPerIndexPerSource();
					Set<TRGTID> trgtids = targetIdPerIndexPerSource.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toSet());
					Set<TRGT> targets = targetPersister.select(trgtids);
					Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetPersisterMapping::getId));
					
					// we sow the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						Map<Integer, TRGTID> targetIdPerIndex = targetIdPerIndexPerSource.get(src);
						if (targetIdPerIndex != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
							Map<Integer, TRGT> targetPerIndex = Maps.innerJoinOnValuesAndKeys(targetIdPerIndex, targetPerId);
							// the values() are sorted thanks to the Map with Integer as key
							relation.getAccessor().get(src).addAll(targetPerIndex.values());
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
			
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
		} else {
			Function<ColumnedRow, Object> duplicateIdentifierProvider;
			Set<Column<RIGHTTABLE, ?>> columnsToSelect = new HashSet<>(targetPersisterMapping.getTargetTable().getPrimaryKey().getColumns());
			columnsToSelect.add(indexingColumn);
			duplicateIdentifierProvider = (columnedRow) -> {
				TRGTID identifier = targetIdentifierAssembler.assemble(columnedRow);
				Integer targetEntityIndex = columnedRow.get(indexingColumn);
				return identifier + "-" + targetEntityIndex;
			};
			
			// Implementation note: we keep the object indexes and put the sorted entities in a temporary Collection, then add them all to the target List
			InMemoryRelationHolder<SRC, SRCID, TRGT, S> inMemoryRelationFixer = new InMemoryRelationHolder<>(
					sourcePersister.getMapping()::getId,
					relation.getAccessor(),
					relation.getComponentFactory(),
					relation.getMappedByAccessor()
			);
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryRelationFixer.init();
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryRelationFixer.applySort(result);
					cleanContext();
				}
				
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					cleanContext();
				}
				
				private void cleanContext() {
					inMemoryRelationFixer.clear();
				}
			});
		
			String manyJoinName = aggregateTree.addRelationJoin(
					mountPoint,
					new EntityMappingAdapter<>(targetPersisterMapping),
					relation.getAccessor(),
					join.getLeftKey(),
					join.getRightKey(),
					null,
					OUTER,
					inMemoryRelationFixer,
					columnsToSelect,
					duplicateIdentifierProvider);
			
			JoinNode<TRGT, Fromable> manyJoinNode = (JoinNode<TRGT, Fromable>) aggregateTree.getJoin(manyJoinName);
			JoinNode<SRC, Fromable> sourceJoinNode = (JoinNode<SRC, Fromable>) aggregateTree.getJoin(mountPoint);
			manyJoinNode.setConsumptionListener((trgt, columnValueProvider) -> {
				ColumnedRow sourceRowDecoder = EntityTreeInflater.currentContext().getDecoder(sourceJoinNode);
				SRCID sourceId = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(sourceRowDecoder);
				Integer index = columnValueProvider.get(indexingColumn);
				inMemoryRelationFixer.addIndex(sourceId, trgt, index);
			});
			
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, aggregateTree);
		}
		return result;
	}
}
