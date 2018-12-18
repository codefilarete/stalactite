package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteByIdTargetEntitiesBeforeDeleteByIdCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.query.model.Operand;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyWithAssociationTableEngine<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>,
		R extends AssociationRecord, T extends AssociationTable> {
	
	/**
	 * Mapping between source entity and records, temporary to the select phase.
	 */
	private final ThreadLocal<Map<I, List<R>>> leftAssociations = new ThreadLocal<>();
	/**
	 * Mapping between record and target entity, temporary to the select phase.
	 */
	private final ThreadLocal<Map<R, O>> rightAssociations = new ThreadLocal<>();
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	protected final PersisterListener<I, J> persisterListener;
	protected final Persister<O, J, ?> targetPersister;
	
	protected final Function<I, C> collectionGetter;
	
	public AbstractOneToManyWithAssociationTableEngine(PersisterListener<I, J> persisterListener, Persister<O, J, ?> targetPersister, Function<I, C> collectionGetter, AssociationRecordPersister<R, T> associationPersister) {
		this.persisterListener = persisterListener;
		this.targetPersister = targetPersister;
		this.collectionGetter = collectionGetter;
		this.associationPersister = associationPersister;
	}
	
	public void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
								 JoinedTablesPersister<I, J, ?> joinedTablesPersister
	) {
		
		IMutator<I, C> collectionSetter = Accessors.<I, C>of(cascadeMany.getMember()).getMutator();
		
		// we must join on the association table and add in-memory reassociation
		// Relation is kept on each row by the "relation fixer" passed to JoinedTablePersister below, because it seems more complex to read it
		// from the Row (as for use case without association table, addTransformerListener(..)) due to the need to create an equivalent
		// structure to AssociationRecord
		// Relation will be fixed after all rows read (SelectListener.afterSelect)
		addIndexReadOnSelect(cascadeMany.getCollectionTargetClass(), persisterListener, collectionSetter);
		// adding association table join
		String associationTableJoinNodeName = joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
				associationPersister,
				(BeanRelationFixer<I, R>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(sourceEntity, record) -> leftAssociations.get().computeIfAbsent(sourceEntity, k -> new ArrayList<>()).add(record),
				associationPersister.getTargetTable().getOneSidePrimaryKey(),
				associationPersister.getTargetTable().getOneSideKeyColumn(),
				true);
		// adding target table join
		joinedTablesPersister.addPersister(associationTableJoinNodeName,
				targetPersister,
				(BeanRelationFixer<R, O>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(record, targetEntity) -> rightAssociations.get().put(record, targetEntity),
				associationPersister.getTargetTable().getManySideKeyColumn(),
				associationPersister.getTargetTable().getManySidePrimaryKey(),
				true);
	}
	
	private void addIndexReadOnSelect(Class<C> collectionTargetClass,
									  PersisterListener<I, J> persisterListener,
									  IMutator<I, C> collectionSetter) {
		// Note: reverse setter does nothing (NOOP) because there's no such a reverse setter in relationship with association table
		BeanRelationFixer<I, O> beanRelationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				collectionTargetClass, OneToManyWithMappedAssociationEngine.NOOP_REVERSE_SETTER);
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				// initializing relation storage, see cleanContext() for deregistration
				leftAssociations.set(new HashMap<>());
				rightAssociations.set(new HashMap<>());
			}
			
			/**
			 * Implementation that assembles source and target beans from ThreadLocal elements thanks to the {@link BeanRelationFixer}
			 * @param result new created beans
			 */
			@Override
			public void afterSelect(Iterable<? extends I> result) {
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
			public void onError(Iterable<J> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				leftAssociations.remove();
				rightAssociations.remove();
			}
		});
	}
	
	public void addInsertCascade() {
		persisterListener.addInsertListener(new TargetInstancesInsertCascader<>(targetPersister, collectionGetter));
		
		persisterListener.addInsertListener(newRecordInsertionCascader(collectionGetter, associationPersister));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevent
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<I, O, C> updateListener = new CollectionUpdater<I, O, C>(collectionGetter, targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onAddedTarget(updateContext, diff);
				R associationRecord = newRecord(updateContext.getPayload().getEntities().getLeft(), diff.getReplacingInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted().add(associationRecord);
			}
			
			@Override
			protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onRemovedTarget(updateContext, diff);
				
				R associationRecord = newRecord(updateContext.getPayload().getEntities().getLeft(), diff.getSourceInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeDeleted().add(associationRecord);
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
				
				private final List<R> associationRecordstoBeInserted = new ArrayList<>();
				private final List<R> associationRecordstoBeDeleted = new ArrayList<>();
				
				public AssociationTableUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
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
		
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public void addDeleteCascade(CascadeMany<I, O, J, C> cascadeMany,
								 JoinedTablesPersister<I, J, ?> joinedTablesPersister,
								 boolean deleteTargetEntities,
								 Dialect dialect) {
		// we delete association records
		persisterListener.addDeleteListener(new DeleteListener<I>() {
			@Override
			public void beforeDelete(Iterable<I> entities) {
				// To be coherent with DeleteListener, we'll delete the association records by ... themselves, not by id.
				// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
				// so it requires that this configurer holds the Dialect which is not the case, but could have.
				// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
				List<R> associationRecords = new ArrayList<>();
				entities.forEach(e -> {
					Collection<O> targets = collectionGetter.apply(e);
					int i = 0;
					for (O target : targets) {
						associationRecords.add(newRecord(e, target, i++));
					}
				});
				// we delete records
				associationPersister.delete(associationRecords);
			}
		});
		
		persisterListener.addDeleteByIdListener(new DeleteByIdListener<I>() {
			
			@Override
			public void beforeDeleteById(Iterable<I> entities) {
				// We delete records thanks to a SQL delete order
				// Yes it is not coherent with beforeDelete(..) !
				// TODO: make it coherent with beforeDelete(..) to avoid dialect and connectionProvider parameters
				Delete<AssociationTable> delete = new Delete<>(associationPersister.getTargetTable());
				ClassMappingStrategy<I, J, ?> mappingStrategy = joinedTablesPersister.getMappingStrategy();
				Set<J> identifiers = collect(entities, mappingStrategy::getId, HashSet::new);
				delete.where(associationPersister.getTargetTable().getOneSideKeyColumn(), Operand.in(identifiers));
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder<>(delete).toStatement(dialect.getColumnBinderRegistry());
				try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, cascadeMany.getPersister().getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
		});
		
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, collectionGetter));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			persisterListener.addDeleteByIdListener(new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, collectionGetter));
		}
	}
	
	protected abstract AfterInsertCollectionCascader<I, R> newRecordInsertionCascader(Function<I, C> collectionGetter,
																					  AssociationRecordPersister<R, T> associationPersister);
	
	protected abstract R newRecord(I e, O target, int index);
}
