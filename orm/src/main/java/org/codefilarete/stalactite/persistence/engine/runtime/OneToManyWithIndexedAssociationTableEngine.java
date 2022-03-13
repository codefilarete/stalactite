package org.codefilarete.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.stalactite.persistence.engine.listener.SelectListener;
import org.codefilarete.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.persistence.id.diff.AbstractDiff;
import org.codefilarete.stalactite.persistence.id.diff.IndexedDiff;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;

import static org.codefilarete.tool.collection.Iterables.first;
import static org.codefilarete.tool.collection.Iterables.minus;
import static org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, IndexedAssociationRecord, IndexedAssociationTable> {
	
	private final Column<IndexedAssociationTable, Integer> indexColumn;
	
	public OneToManyWithIndexedAssociationTableEngine(ConfiguredJoinedTablesPersister<SRC, SRCID> joinedTablesPersister,
													  EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
													  ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													  AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> associationPersister,
													  Column<IndexedAssociationTable, Integer> indexColumn,
													  WriteOperationFactory writeOperationFactory) {
		super(joinedTablesPersister, targetPersister, manyRelationDescriptor, associationPersister, writeOperationFactory);
		this.indexColumn = indexColumn;
	}
	
	public void addSelectCascade(EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister) {
		
		// we join on the association table and add bean association in memory
		String associationTableJoinNodeName = sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				JoinType.OUTER, (Set) Arrays.asHashSet(indexColumn));
		
		// we add target subgraph joins to main persister
		targetPersister.joinAsMany(sourcePersister, associationPersister.getMainTable().getManySideKeyColumn(),
				associationPersister.getMainTable().getManySidePrimaryKey(), manyRelationDescriptor.getRelationFixer(),
				(row, columnedRow) -> {
					TRGTID identifier = targetPersister.getMappingStrategy().getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow);
					Integer targetEntityIndex = EntityTreeInflater.currentContext().getRowDecoder().giveValue(associationTableJoinNodeName, indexColumn, row);
					return identifier + "-" + targetEntityIndex;
				}, associationTableJoinNodeName, true);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				Iterable collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
						.map(Collection::stream)
						.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved, boolean maintainAssociationOnly) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevent
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, C> updateListener = new CollectionUpdater<SRC, TRGT, C>(manyRelationDescriptor.getCollectionGetter(), targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onHeldTarget(updateContext, diff);
				IndexedDiff indexedDiff = (IndexedDiff) diff;
				Set<Integer> minus = minus(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
				Integer index = first(minus);
				if (index != null ) {
					SRC leftIdentifier = updateContext.getPayload().getLeft();
					PairIterator<Integer, Integer> diffIndexIterator = new PairIterator<>(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
					diffIndexIterator.forEachRemaining(d -> {
						if (!d.getLeft().equals(d.getRight()))
							((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeUpdated().add(new Duo<>(
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getLeft()),
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getRight())));
					});
				}
			}
			
			@Override
			protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
				return getDiffer().diffList((List<TRGT>) unmodified, (List<TRGT>) modified);
			}
			
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onAddedTarget(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getReplacerIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted().add(
								newRecord(leftIdentifier, diff.getReplacingInstance(), idx)));
			}
			
			@Override
			protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onRemovedTarget(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getSourceIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeDeleted().add(
								newRecord(leftIdentifier, diff.getSourceInstance(), idx)));
			}
			
			@Override
			protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
				super.updateTargets(updateContext, allColumnsStatement);
				// we ask for index update : all columns shouldn't be updated, only index, so we don't need "all columns in statement"
				associationPersister.update(((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeUpdated(), false);
			}
			
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				// we insert targets before association records to satisfy integrity constraint
				super.insertTargets(updateContext);
				associationPersister.insert(((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted());
				
			}
			
			@Override
			protected void deleteTargets(UpdateContext updateContext) {
				// we delete association records before targets to satisfy integrity constraint
				associationPersister.delete(((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeDeleted());
				super.deleteTargets(updateContext);
			}
			
			class AssociationTableUpdateContext extends UpdateContext {
				
				private final List<IndexedAssociationRecord> associationRecordstoBeInserted = new ArrayList<>();
				private final List<IndexedAssociationRecord> associationRecordstoBeDeleted = new ArrayList<>();
				private final List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> associationRecordstoBeUpdated = new ArrayList<>();
				
				public AssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordstoBeInserted() {
					return associationRecordstoBeInserted;
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordstoBeDeleted() {
					return associationRecordstoBeDeleted;
				}
				
				public List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> getAssociationRecordstoBeUpdated() {
					return associationRecordstoBeUpdated;
				}
			}
		};
		
		// Can we cascade update on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
		}
	}
	
	@Override
	protected IndexedAssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> associationPersister,
			EntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
			EntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		return new IndexedAssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected IndexedAssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new IndexedAssociationRecord(sourcePersister.getMappingStrategy().getId(e), targetPersister.getMappingStrategy().getId(target), index);
	}
}