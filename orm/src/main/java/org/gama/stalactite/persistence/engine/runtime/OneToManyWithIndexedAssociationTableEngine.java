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
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;

import static org.gama.lang.collection.Iterables.first;
import static org.gama.lang.collection.Iterables.minus;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedAssociationTableEngine<I extends Identified, O extends Identified, J extends Identifier, C extends List<O>>
		extends AbstractOneToManyWithAssociationTableEngine<I, O, J, C, IndexedAssociationRecord, IndexedAssociationTable> {
	
	public OneToManyWithIndexedAssociationTableEngine(PersisterListener<I, J> persisterListener,
													  Persister<O, J, ?> leftPersister,
													  ManyRelationDescriptor<I, O, C> manyRelationDescriptor,
													  AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> associationPersister) {
		super(persisterListener, leftPersister, manyRelationDescriptor, associationPersister);
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevent
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<I, O, C> updateListener = new CollectionUpdater<I, O, C>(manyRelationDescriptor.getCollectionGetter(), targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onHeldTarget(updateContext, diff);
				IndexedDiff indexedDiff = (IndexedDiff) diff;
				Set<Integer> minus = minus(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
				Integer index = first(minus);
				if (index != null ) {
					I leftIdentifier = updateContext.getPayload().getEntities().getLeft();
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
			protected Set<? extends AbstractDiff<O>> diff(Collection<O> modified, Collection<O> unmodified) {
				return getDiffer().diffList((List<O>) unmodified, (List<O>) modified);
			}
			
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onAddedTarget(updateContext, diff);
				I leftIdentifier = updateContext.getPayload().getEntities().getLeft();
				((IndexedDiff<O>) diff).getReplacerIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted().add(
								newRecord(leftIdentifier, diff.getReplacingInstance(), idx)));
			}
			
			@Override
			protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onRemovedTarget(updateContext, diff);
				I leftIdentifier = updateContext.getPayload().getEntities().getLeft();
				((IndexedDiff<O>) diff).getSourceIndexes().forEach(idx ->
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
				
				public AssociationTableUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
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
		
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	@Override
	protected IndexedAssociationRecordInsertionCascader<I, O, C> newRecordInsertionCascader(Function<I, C> collectionGetter,
																							AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> associationPersister) {
		return new IndexedAssociationRecordInsertionCascader<>(associationPersister, collectionGetter);
	}
	
	@Override
	protected IndexedAssociationRecord newRecord(I e, O target, int index) {
		return new IndexedAssociationRecord(e.getId(), target.getId(), index);
	}
}