package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateOneToManyAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE> append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                            EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		Holder<GraftPoint<TRGT, TRGTID, RIGHTTABLE>> resultHolder = new Holder<>();
		
		if (relation.isOwnedByReverseSide()) {
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
				duplicateIdentifierProvider = (columnedRow) -> {
					TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
					return identifier;
				};
			}
			DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) relation.getJoin();
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
			
			// Preparing for next iteration
			// Note that we can't set the correct generics types to the GraftPoint instance
			// because we go a step further in the relation by shifting the types from SRC to TRGT 
			resultHolder.set(new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName));
		} else {
			String manyJoinName;
			if (relation.isOrdered()) {
				manyJoinName = appendIndexedAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint);
			} else {
				manyJoinName = appendAssociation(targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint);
			}
			
			// Preparing for next iteration
			// Note that we can't set the correct generics types to the GraftPoint instance
			// because we go a step further in the relation by shifting the types from SRC to TRGT 
			resultHolder.set(new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName));
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
		return resultHolder.get();
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendAssociation(EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                         ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                         PropertyAccessor<SRC, S> accessor,
	                         EntityJoinTree<SRC, SRCID> entityJoinTree,
	                         String mountPoint) {
		Function<ColumnedRow, Object> duplicateIdentifierProvider = null;
		Set<Column<ASSOCIATIONTABLE, Integer>> columnsToSelect = Collections.emptySet();
		
		// we join on the association table
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		String associationTableJoinName = entityJoinTree.addPassiveJoin(
				mountPoint,
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				columnsToSelect);
		
		return entityJoinTree.addRelationJoin(
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
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendIndexedAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                PropertyAccessor<SRC, S> accessor,
	                                EntityJoinTree<SRC, SRCID> entityJoinTree,
	                                String mountPoint) {
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		Holder<String> associationTableJoinNodeNameHolder = new Holder<>();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = (Column<ASSOCIATIONTABLE, Integer>) relation.<IndexedAssociationTable>getIndexingAssociationColumn();
		Function<ColumnedRow, Object> duplicateIdentifierProvider = (columnedRow) -> {
			TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
			// indexColumn column value is took on join of association table, not target table, so we have to grab it
			JoinNode<IndexedAssociationRecord, Fromable> joinNode = (JoinNode<IndexedAssociationRecord, Fromable>) entityJoinTree.getJoin(associationTableJoinNodeNameHolder.get());
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(joinNode);
			Integer targetEntityIndex = rowDecoder.get(indexingColumn);
			return identifier + "-" + targetEntityIndex;
		};
		
		// we join on the association table
		String associationTableJoinName = entityJoinTree.addPassiveJoin(
				mountPoint,
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				// we must add all the columns to make them available while decoding the row to create an IndexedAssociationRecord
				join.getJoinTable().getColumns());
		associationTableJoinNodeNameHolder.set(associationTableJoinName);
		
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
		
		String manyJoinName = entityJoinTree.addRelationJoin(
				associationTableJoinName,
				new EntityMappingAdapter<>(targetPersister.getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				inMemoryRelationFixer,
				Collections.emptySet(),
				duplicateIdentifierProvider);
		
		JoinNode<TRGT, Fromable> joinNode = (JoinNode<TRGT, Fromable>) entityJoinTree.getJoin(manyJoinName);
		JoinNode<?, Fromable> associationJoinNode = entityJoinTree.getJoin(associationTableJoinName);
		IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new IndexedAssociationRecordMapping<>(
				join.getJoinTable(),
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
				join.getJoinTable().getLeftIdentifierColumnMapping(),
				join.getJoinTable().getRightIdentifierColumnMapping());
		joinNode.setConsumptionListener((trgt, columnValueProvider) -> {
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(associationJoinNode);
			IndexedAssociationRecord associationRecord = associationRecordMapping.getRowTransformer().transform(rowDecoder);
			inMemoryRelationFixer.addIndex((SRCID) associationRecord.getLeft(), trgt, associationRecord.getIndex());
		});
		
		return manyJoinName;
	}
}
