package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.IndexedAssociationRecord;
import org.gama.stalactite.persistence.engine.IndexedAssociationTable;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;

import static org.gama.lang.collection.Iterables.first;
import static org.gama.lang.collection.Iterables.minus;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, IndexedAssociationRecord, IndexedAssociationTable> {
	
	public OneToManyWithIndexedAssociationTableEngine(JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister,
													  JoinedTablesPersister<TRGT, TRGTID, ?> targetPersister,
													  ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													  AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> associationPersister) {
		super(joinedTablesPersister, targetPersister, manyRelationDescriptor, associationPersister);
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
				// we insert association records before targets to satisfy integrity constraint
				associationPersister.insert(((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted());
				super.insertTargets(updateContext);
				
			}
			
			@Override
			protected void deleteTargets(UpdateContext updateContext) {
				super.deleteTargets(updateContext);
				associationPersister.delete(((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeDeleted());
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
			IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
			IEntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		return new IndexedAssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected IndexedAssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new IndexedAssociationRecord(joinedTablesPersister.getMappingStrategy().getId(e), targetPersister.getMappingStrategy().getId(target), index);
	}
}