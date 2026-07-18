package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.IndexedDiff;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecordInsertionCascader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.tool.collection.Iterables.first;
import static org.codefilarete.tool.collection.Iterables.minus;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedAssociationTableEngine<
		SRC,
		TRGT,
		SRCID,
		TRGTID,
		S extends Collection<TRGT>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		extends OneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, S, IndexedAssociationRecord, ASSOCIATIONTABLE> {

	public OneToManyWithIndexedAssociationTableEngine(EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                                                  EntityWriteExecutor<TRGT, TRGTID> targetPersister,
													  IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID> manyRelationDescriptor,
													  AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> associationPersister,
													  WriteOperationFactory writeOperationFactory,
													  Dialect dialect) {
		super(sourcePersister, targetPersister, manyRelationDescriptor, associationPersister, writeOperationFactory, dialect);
	}
	
	@Override
	public IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID> getManyRelationDescriptor() {
		return (IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID>) super.getManyRelationDescriptor();
	}
	
	@Override
	public void addUpdateCascade(EntityWriteExecutor<TRGT, TRGTID> targetPersister) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevant
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, S> collectionUpdater = new CollectionUpdater<SRC, TRGT, S>(manyRelationDescriptor.getCollectionAccessPoint(), targetPersister, null, getManyRelationDescriptor().isOrphanRemoval()) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onHeldElements(updateContext, diff);
				IndexedDiff indexedDiff = (IndexedDiff) diff;
				Set<Integer> minus = minus(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
				Integer index = first(minus);
				if (index != null ) {
					SRC leftIdentifier = updateContext.getPayload().getLeft();
					PairIterator<Integer, Integer> diffIndexIterator = new PairIterator<>(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
					diffIndexIterator.forEachRemaining(d -> {
						if (!d.getLeft().equals(d.getRight()))
							((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated().add(new Duo<>(
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getLeft()),
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getRight())));
					});
				}
			}
			
			@Override
			protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
				return getDiffer().diffOrdered(unmodified, modified);
			}
			
			@Override
			protected void onAddedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onAddedElements(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getReplacerIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(
								newRecord(leftIdentifier, diff.getReplacingInstance(), idx)));
			}
			
			@Override
			protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onRemovedElements(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getSourceIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(
								newRecord(leftIdentifier, diff.getSourceInstance(), idx)));
			}
			
			@Override
			protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
				super.updateTargets(updateContext, allColumnsStatement);
				// association records can't be updated because they are primary key elements for us (see EmbeddedClassMapping and its updatableProperties computation), 
				// so we ask for their deletion + creation
				List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> associationRecordsToBeUpdated = ((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated();
				associationPersister.delete(Iterables.collectToList(associationRecordsToBeUpdated, Duo::getRight));
				associationPersister.insert(Iterables.collectToList(associationRecordsToBeUpdated, Duo::getLeft));
			}
			
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				// we insert targets before association records to satisfy integrity constraint
				super.insertTargets(updateContext);
				associationPersister.insert(((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted());
				
			}
			
			@Override
			protected void deleteTargets(UpdateContext updateContext) {
				// we delete association records before targets to satisfy integrity constraint
				associationPersister.delete(((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted());
				super.deleteTargets(updateContext);
			}
			
			class AssociationTableUpdateContext extends UpdateContext {
				
				private final List<IndexedAssociationRecord> associationRecordsToBeInserted = new ArrayList<>();
				private final List<IndexedAssociationRecord> associationRecordsToBeDeleted = new ArrayList<>();
				private final List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> associationRecordsToBeUpdated = new ArrayList<>();
				
				public AssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordsToBeInserted() {
					return associationRecordsToBeInserted;
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordsToBeDeleted() {
					return associationRecordsToBeDeleted;
				}
				
				public List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> getAssociationRecordsToBeUpdated() {
					return associationRecordsToBeUpdated;
				}
			}
		};
		
		// Can we cascade update on target entities ? it depends on relation maintenance mode
		if (!getManyRelationDescriptor().isMaintainAssociationOnly()) {
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
		}
	}
	
	@Override
	protected IndexedAssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, S> newRecordInsertionCascader(
			Accessor<SRC, S> collectionGetter,
			AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> associationPersister,
			EntityMapping<SRC, SRCID, ?> mappingStrategy,
			EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
		return new IndexedAssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected IndexedAssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new IndexedAssociationRecord(sourcePersister.getMapping().getId(e), targetPersister.getMapping().getId(target), index);
	}
}
