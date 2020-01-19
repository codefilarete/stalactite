package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.configurer.PersisterDispatcher;
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

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, ID, C extends Collection<TRGT>,
		R extends AssociationRecord, T extends AssociationTable> {
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	protected final PersisterListener<SRC, ID> persisterListener;
	
	protected final IConfiguredPersister<SRC, ID> joinedTablesPersister;
	
	protected final IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public AbstractOneToManyWithAssociationTableEngine(IConfiguredPersister<SRC, ID> joinedTablesPersister,
													   IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
													   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													   AssociationRecordPersister<R, T> associationPersister) {
		this.joinedTablesPersister = joinedTablesPersister;
		this.persisterListener = joinedTablesPersister.getPersisterListener();
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.associationPersister = associationPersister;
	}
	
	public void addSelectCascade(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister) {
		
		// we must join on the association table and add in-memory reassociation
		String associationTableJoinNodeName = sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(FIRST_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				JoinType.OUTER, (Set) Collections.emptySet());
		
		// Note: reverse setter does nothing (NOOP) because there's no such a reverse setter in relation with association table
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = BeanRelationFixer.of(
				manyRelationDescriptor.getCollectionSetter(),
				manyRelationDescriptor.getCollectionGetter(),
				manyRelationDescriptor.getCollectionFactory(),
				Nullable.nullable(manyRelationDescriptor.getReverseSetter()).getOr(OneToManyWithMappedAssociationEngine.NOOP_REVERSE_SETTER));
		
		
		if (targetPersister instanceof PersisterDispatcher) {
			// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
			// (we don't need to create bean nor fulfill properties in first phase) 
			// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
			String createdJoinNodeName = sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(associationTableJoinNodeName,
					associationPersister.getMainTable().getManySideKeyColumn(),
					associationPersister.getMainTable().getManySidePrimaryKey(),
					JoinType.OUTER, (Set) Collections.emptySet());
			
			((PersisterDispatcher<TRGT, ID>) targetPersister).joinWithMany(sourcePersister,
					associationPersister.getMainTable().getManySideKeyColumn(),
					beanRelationFixer, createdJoinNodeName);
		} else {
			String createdJoinNodeName = sourcePersister.addPersister(associationTableJoinNodeName,
					targetPersister,
					beanRelationFixer,
					associationPersister.getMainTable().getManySideKeyColumn(),
					associationPersister.getMainTable().getManySidePrimaryKey(),
					true);
			
			// adding target subgraph select to source persister
			targetPersister.copyJoinsRootTo(sourcePersister.getJoinedStrategiesSelect(), createdJoinNodeName);
		}
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, ID>() {
			@Override
			public void beforeSelect(Iterable<ID> ids) {
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
			public void onError(Iterable<ID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	public String addSelectCascade2(IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister) {
		
		// we must join on the association table and add in-memory reassociation
		String associationTableJoinNodeName = sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(FIRST_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				JoinType.OUTER, (Set) Collections.emptySet());
			
		// Note: reverse setter does nothing (NOOP) because there's no such a reverse setter in relation with association table
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = BeanRelationFixer.of(
				manyRelationDescriptor.getCollectionSetter(),
				manyRelationDescriptor.getCollectionGetter(),
				manyRelationDescriptor.getCollectionFactory(),
				OneToManyWithMappedAssociationEngine.NOOP_REVERSE_SETTER);
		
		// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
		// (we don't need to create bean nor fulfill properties in first phase) 
		// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
		String createdJoinNodeName = sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(associationTableJoinNodeName,
				associationPersister.getMainTable().getManySideKeyColumn(),
				associationPersister.getMainTable().getManySidePrimaryKey(),
				JoinType.OUTER, (Set) Collections.emptySet());
			
		((PersisterDispatcher<TRGT, ID>) targetPersister).joinWithMany(sourcePersister,
				associationPersister.getMainTable().getManySideKeyColumn(),
				beanRelationFixer, createdJoinNodeName);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, ID>() {
			@Override
			public void beforeSelect(Iterable<ID> ids) {
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
			public void onError(Iterable<ID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
		
		return createdJoinNodeName;
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
				Set<ID> identifiers = collect(entities, this::castId, HashSet::new);
				delete.where(associationPersister.getMainTable().getOneSideKeyColumn(), Operators.in(identifiers));
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder<>(delete).toStatement(columnBinderRegistry);
				try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, associationPersister.getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
			
			private ID castId(SRC e) {
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
																						IEntityMappingStrategy<SRC, ID, ?> mappingStrategy,
																						IEntityMappingStrategy<TRGT, ID, ?> strategy);
	
	protected abstract R newRecord(SRC e, TRGT target, int index);
}
