package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseIndexedManyRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseIndexedManyRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalIndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyWithIndexedMappedAssociationAppender {
	
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
		Column<RIGHTTABLE, Integer> indexingColumn = relation.getIndexingMappedColumn();
		
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetPersisterMapping = targetReader.getMapping();
		IdentifierAssembler<TRGTID, ?> targetIdentifierAssembler = targetPersisterMapping.getIdMapping().getIdentifierAssembler();
		if (relation.isFetchedSeparately()) {
			result = appendWithSeparateFetch(relation, sourceReader, targetReader, mountPoint, aggregateTree, targetIdentifierAssembler, indexingColumn, join);
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
					sourceReader.getMapping()::getId,
					relation.getAccessor(),
					relation.getComponentFactory(),
					relation.getMappedByAccessor()
			);
			sourceReader.addSelectListener(new SelectListener<SRC, SRCID>() {
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
				SRCID sourceId = sourceReader.getMapping().getIdMapping().getIdentifierAssembler().assemble(sourceRowDecoder);
				Integer index = columnValueProvider.get(indexingColumn);
				inMemoryRelationFixer.addIndex(sourceId, trgt, index);
			});
			
			result = new GraftPoint(relation.getTargetEntity(), targetReader, manyJoinName, aggregateTree);
		}
		return result;
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID>
	appendWithSeparateFetch(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
							ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
							ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
							String mountPoint,
							EntityJoinTree<SRC, SRCID> aggregateTree,
							IdentifierAssembler<TRGTID, ?> targetIdentifierAssembler,
							Column<RIGHTTABLE, Integer> indexingColumn,
							DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join) {
		ThreadLocalIndexedRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalIndexedRelationStorage<>();
		
		// here is the logic below :
		// - we collect the SRC-Index-TRGTID on the join: see FirstPhaseIndexedManyRelationLoader usage hereafter
		// - then we trigger the target entities collect on the afterSelect of the source, which can apply the relation
		FirstPhaseIndexedManyRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseIndexedManyRelationLoader<>(
				targetIdentifierAssembler::assemble,
				targetIdentifierAssembler.getColumns(),
				current2PhasesLoadContext,
				indexingColumn);
		Set<Selectable<?>> selectableColumns = new KeepOrderSet<>(firstPhaseCollector.getSelectableColumns());
		// we add the index column to make it available to the first-phase loader
		selectableColumns.add(indexingColumn);
		aggregateTree.addMergeJoin(mountPoint,
				// we give the targetIdentifierAssembler column to ensure it can assemble the id from the ColumnedRow
				firstPhaseCollector,
				join.getLeftKey(),
				join.getRightKey(),
				OUTER,
				selectableColumns);
		
		// adding second phase loader
		sourceReader.addSelectListener(new SecondPhaseIndexedManyRelationSelectListener<>(
				firstPhaseCollector,
				targetReader.getMapping()::getId,
				targetReader,
				relation.getRelationFixer()::apply
		));
		
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, targetReader.getEntityJoinTree());
	}
}
