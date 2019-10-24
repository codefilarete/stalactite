package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteByIdTargetEntitiesBeforeDeleteByIdCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.WriteOperation;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;
import static org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.addSubgraphSelect;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>,
		R extends AssociationRecord, T extends AssociationTable> {
	
	/**
	 * Mapping between source entity and records, temporary to the select phase.
	 */
	private final ThreadLocal<Map<SRC, List<R>>> leftAssociations = new ThreadLocal<>();
	/**
	 * Mapping between record and target entity, temporary to the select phase.
	 */
	private final ThreadLocal<Map<R, TRGT>> rightAssociations = new ThreadLocal<>();
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	protected final PersisterListener<SRC, SRCID> persisterListener;
	
	protected final JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister;
	
	protected final JoinedTablesPersister<TRGT, TRGTID, ?> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public AbstractOneToManyWithAssociationTableEngine(JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister,
													   JoinedTablesPersister<TRGT, TRGTID, ?> targetPersister,
													   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													   AssociationRecordPersister<R, T> associationPersister) {
		this.joinedTablesPersister = joinedTablesPersister;
		this.persisterListener = joinedTablesPersister.getPersisterListener();
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.associationPersister = associationPersister;
	}
	
	public void addSelectCascade(JoinedTablesPersister<SRC, SRCID, ?> sourcePersister) {
		
		// we must join on the association table and add in-memory reassociation
		// Relation is kept on each row by the "relation fixer" passed to JoinedTablePersister below, because it seems more complex to read it
		// from the Row (as for use case without association table, addTransformerListener(..)) due to the need to create an equivalent
		// structure to AssociationRecord
		// Relation will be fixed after all rows read (SelectListener.afterSelect)
		addRelationReadOnSelect();
		// adding association table join
		String associationTableJoinNodeName = sourcePersister.addPersister(FIRST_STRATEGY_NAME,
				associationPersister,
				(BeanRelationFixer<SRC, R>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(sourceEntity, record) -> leftAssociations.get().computeIfAbsent(sourceEntity, k -> new ArrayList<>()).add(record),
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				true);
		// adding target table join
		String createdJoinNodeName = sourcePersister.addPersister(associationTableJoinNodeName,
				targetPersister,
				(BeanRelationFixer<R, TRGT>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(record, targetEntity) -> rightAssociations.get().put(record, targetEntity),
				associationPersister.getMainTable().getManySideKeyColumn(),
				associationPersister.getMainTable().getManySidePrimaryKey(),
				true);
		
		addSubgraphSelect(createdJoinNodeName, sourcePersister, targetPersister, manyRelationDescriptor.getCollectionGetter());
	}
	
	private void addRelationReadOnSelect() {
		// Note: reverse setter does nothing (NOOP) because there's no such a reverse setter in relationship with association table
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = BeanRelationFixer.of(
				manyRelationDescriptor.getCollectionSetter(),
				manyRelationDescriptor.getCollectionGetter(),
				manyRelationDescriptor.getCollectionFactory(),
				OneToManyWithMappedAssociationEngine.NOOP_REVERSE_SETTER);
		persisterListener.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// initializing relation storage, see cleanContext() for deregistration
				leftAssociations.set(new HashMap<>());
				rightAssociations.set(new HashMap<>());
			}
			
			/**
			 * Implementation that assembles source and target beans from ThreadLocal elements thanks to the {@link BeanRelationFixer}
			 * @param result new created beans
			 */
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				try {
					result.forEach(bean -> {
						List<R> associationRecords = leftAssociations.get().get(bean);
						if (associationRecords != null) {
							associationRecords.forEach(r -> beanRelationFixer.apply(bean, rightAssociations.get().get(r)));
						} // else : no related bean found in database, nothing to do, collection is empty
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				leftAssociations.remove();
				rightAssociations.remove();
			}
		});
	}
	
	public void addInsertCascade(boolean maintainAssociationOnly) {
		// Can we cascade insert on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addInsertListener(new TargetInstancesInsertCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		}
		
		persisterListener.addInsertListener(newRecordInsertionCascader(
				manyRelationDescriptor.getCollectionGetter(),
				associationPersister,
				joinedTablesPersister.getMappingStrategy(),
				targetPersister.getMappingStrategy()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved, boolean maintainAssociationOnly) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevent
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, C> updateListener = new CollectionUpdater<SRC, TRGT, C>(manyRelationDescriptor.getCollectionGetter(), targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onAddedTarget(updateContext, diff);
				R associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted().add(associationRecord);
			}
			
			@Override
			protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onRemovedTarget(updateContext, diff);
				
				R associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeDeleted().add(associationRecord);
			}
			
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				// we insert association records after targets to satisfy integrity constraint
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
				
				private final List<R> associationRecordstoBeInserted = new ArrayList<>();
				private final List<R> associationRecordstoBeDeleted = new ArrayList<>();
				
				public AssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public List<R> getAssociationRecordstoBeInserted() {
					return associationRecordstoBeInserted;
				}
				
				public List<R> getAssociationRecordstoBeDeleted() {
					return associationRecordstoBeDeleted;
				}
			}
		};
		
		// Can we cascade update on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
		}
	}
	
	/**
	 * Add deletion of association records on {@link Persister#delete} and {@link Persister#deleteById} events.
	 * If {@code deleteTargetEntities} is true, then will also delete target (many side) entities.
	 * 
	 * In case of {@link Persister#deleteById}, association records will be deleted only by source entity keys.
	 * 
	 * @param deleteTargetEntities true to delete many-side entities, false to onlly delete association records
	 * @param columnBinderRegistry should come from a {@link org.gama.stalactite.persistence.sql.Dialect}, necessary for deleteById action
	 */
	public void addDeleteCascade(boolean deleteTargetEntities, ColumnBinderRegistry columnBinderRegistry) {
		// we delete association records
		persisterListener.addDeleteListener(new DeleteListener<SRC>() {
			@Override
			public void beforeDelete(Iterable<SRC> entities) {
				// To be coherent with DeleteListener, we'll delete the association records by ... themselves, not by id.
				// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
				// so it requires that this configurer holds the Dialect which is not the case, but could have.
				// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
				List<R> associationRecords = new ArrayList<>();
				entities.forEach(e -> {
					Collection<TRGT> targets = manyRelationDescriptor.getCollectionGetter().apply(e);
					int i = 0;
					for (TRGT target : targets) {
						associationRecords.add(newRecord(e, target, i++));
					}
				});
				// we delete records
				associationPersister.delete(associationRecords);
			}
		});
		
		persisterListener.addDeleteByIdListener(new DeleteByIdListener<SRC>() {
			
			@Override
			public void beforeDeleteById(Iterable<SRC> entities) {
				// We delete association records by entity keys, not their id because we don't have them (it is themselves and we don't have the full
				// entities, only their id)
				// We do it thanks to a SQL delete order ... not very coherent with beforeDelete(..) !
				Delete<AssociationTable> delete = new Delete<>(associationPersister.getMainTable());
				Set<SRCID> identifiers = collect(entities, this::castId, HashSet::new);
				delete.where(associationPersister.getMainTable().getOneSideKeyColumn(), Operators.in(identifiers));
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder<>(delete).toStatement(columnBinderRegistry);
				try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, associationPersister.getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
			
			private SRCID castId(SRC e) {
				return joinedTablesPersister.getMappingStrategy().getId(e);
			}
		});
		
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			persisterListener.addDeleteByIdListener(new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		}
	}
	
	protected abstract AfterInsertCollectionCascader<SRC, R> newRecordInsertionCascader(Function<SRC, C> collectionGetter,
																						AssociationRecordPersister<R, T> associationPersister,
																						IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
																						IEntityMappingStrategy<TRGT, TRGTID, ?> strategy);
	
	protected abstract R newRecord(SRC e, TRGT target, int index);
}
