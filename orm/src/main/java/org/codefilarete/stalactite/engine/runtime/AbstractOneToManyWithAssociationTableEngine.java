package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteByIdTargetEntitiesBeforeDeleteByIdCascader;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.codefilarete.stalactite.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.engine.configurer.CascadeManyConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

import static org.codefilarete.tool.collection.Iterables.collect;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, R extends AssociationRecord, T extends AssociationTable> {
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	protected final PersisterListenerCollection<SRC, SRCID> persisterListener;
	
	protected final ConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
	
	protected final EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	private final WriteOperationFactory writeOperationFactory;
	
	public AbstractOneToManyWithAssociationTableEngine(ConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
													   EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
													   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													   AssociationRecordPersister<R, T> associationPersister,
													   WriteOperationFactory writeOperationFactory) {
		this.sourcePersister = sourcePersister;
		this.persisterListener = sourcePersister.getPersisterListener();
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.associationPersister = associationPersister;
		this.writeOperationFactory = writeOperationFactory;
	}
	
	public ManyRelationDescriptor<SRC, TRGT, C> getManyRelationDescriptor() {
		return manyRelationDescriptor;
	}
	
	public void addSelectCascade(EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister) {
		
		// we join on the association table and add bean association in memory
		String associationTableJoinNodeName = sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				JoinType.OUTER, (Set) Collections.emptySet());
		
		// we add target subgraph joins to main persister
		targetPersister.joinAsMany(sourcePersister, associationPersister.getMainTable().getManySideKeyColumn(),
				associationPersister.getMainTable().getManySidePrimaryKey(), manyRelationDescriptor.getRelationFixer(), null, associationTableJoinNodeName, true);
		
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
	
	public void addInsertCascade(boolean maintainAssociationOnly) {
		// Can we cascade insert on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addInsertListener(new TargetInstancesInsertCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		}
		
		persisterListener.addInsertListener(newRecordInsertionCascader(
				manyRelationDescriptor.getCollectionGetter(),
				associationPersister,
				sourcePersister.getMappingStrategy(),
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
	 * @param columnBinderRegistry should come from a {@link Dialect}, necessary for deleteById action
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
				Delete associationTableDelete = new Delete(associationPersister.getMainTable());
				Set<SRCID> identifiers = collect(entities, this::castId, HashSet::new);
				associationTableDelete.where(associationPersister.getMainTable().getOneSideKeyColumn(), Operators.in(identifiers));
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder(associationTableDelete).toStatement(columnBinderRegistry);
				// We don't know how many relations is contained in the table, so we don't check for deletion row count
				try (WriteOperation<Integer> writeOperation = writeOperationFactory.createInstance(deleteStatement, associationPersister.getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
			
			private SRCID castId(SRC e) {
				return sourcePersister.getMappingStrategy().getId(e);
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
																						EntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
																						EntityMappingStrategy<TRGT, TRGTID, ?> strategy);
	
	protected abstract R newRecord(SRC e, TRGT target, int index);
	
	public void addSelectCascadeIn2Phases(FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		// we join on the association table and add bean association in memory
		sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSidePrimaryKey(),
				associationPersister.getMainTable().getOneSideKeyColumn(),
				JoinType.OUTER, (Set) Arrays.asSet(associationPersister.getMainTable().getManySideKeyColumn()),
				(src, rowValueProvider) -> firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, (TRGTID) rowValueProvider.apply(associationPersister.getMainTable().getManySideKeyColumn())));
	}
}
