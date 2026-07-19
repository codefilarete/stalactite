package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordInsertionCascader;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.Operators;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.collect;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, R extends AssociationRecord, T extends AssociationTable<T, ?, ?, SRCID, TRGTID>>
		extends AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, C> {
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	private final WriteOperationFactory writeOperationFactory;
	
	/** necessary to build valid SQL for deleteById action */
	private final Dialect dialect;
	
	public OneToManyWithAssociationTableEngine(EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                                           EntityReadWriteExecutor<TRGT, TRGTID> targetPersister,
	                                           ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
	                                           AssociationRecordPersister<R, T> associationPersister,
	                                           WriteOperationFactory writeOperationFactory,
	                                           Dialect dialect) {
		super(sourcePersister, targetPersister, manyRelationDescriptor);
		this.associationPersister = associationPersister;
		this.writeOperationFactory = writeOperationFactory;
		this.dialect = dialect;
	}
	
	@Override
	public void addInsertCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister) {
		// Can we cascade insert on target entities ? it depends on relation maintenance mode
		if (!getManyRelationDescriptor().isMaintainAssociationOnly()) {
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
		}
		
		sourcePersister.addInsertListener(newRecordInsertionCascader(
				manyRelationDescriptor.getCollectionAccessPoint(),
				associationPersister,
				sourcePersister.getMapping(),
				targetPersister.getMapping()));
	}
	
	@Override
	public void addUpdateCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister) {
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevant
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, C> collectionUpdater = new CollectionUpdater<SRC, TRGT, C>(manyRelationDescriptor.getCollectionAccessPoint(), targetPersister, null, getManyRelationDescriptor().isOrphanRemoval()) {
			@Override
			protected AssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onAddedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onAddedElements(updateContext, diff);
				R associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(associationRecord);
			}
			
			@Override
			protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onRemovedElements(updateContext, diff);
				
				R associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance(), 0);
				((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(associationRecord);
			}
			
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				// we insert association records after targets to satisfy integrity constraint
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
				
				private final List<R> associationRecordsToBeInserted = new ArrayList<>();
				private final List<R> associationRecordsToBeDeleted = new ArrayList<>();
				
				public AssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public List<R> getAssociationRecordsToBeInserted() {
					return associationRecordsToBeInserted;
				}
				
				public List<R> getAssociationRecordsToBeDeleted() {
					return associationRecordsToBeDeleted;
				}
			}
		};
		
		// Can we cascade update on target entities ? it depends on relation maintenance mode
		if (!getManyRelationDescriptor().isMaintainAssociationOnly()) {
			sourcePersister.addUpdateListener(new OneToManyWithMappedAssociationEngine.AfterUpdateTrigger<>(collectionUpdater));
		}
	}
	
	/**
	 * Adds deletion of association records on {@link BeanPersister#delete} and {@link BeanPersister#deleteById} events.
	 * If {@code deleteTargetEntities} is true, then will also delete target (many side) entities.
	 *
	 * In case of {@link BeanPersister#deleteById}, association records will be deleted only by source entity keys.
	 */
	@Override
	public void addDeleteCascade(EntityWriteExecutor<TRGT, TRGTID> targetPersister) {
		// we delete association records
		sourcePersister.addDeleteListener(new DeleteListener<SRC>() {
			@Override
			public void beforeDelete(Iterable<? extends SRC> entities) {
				// To be coherent with DeleteListener, we'll delete the association records by ... themselves, not by id.
				// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
				// so it requires that this configurer holds the Dialect which is not the case, but could have.
				// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
				List<R> associationRecords = new ArrayList<>();
				entities.forEach(src -> {
					Collection<TRGT> targets = nullable(manyRelationDescriptor.getCollectionAccessPoint().get(src)).getOr(manyRelationDescriptor.getCollectionFactory());
					int i = INDEXED_COLLECTION_FIRST_INDEX_VALUE;
					for (TRGT target : targets) {
						associationRecords.add(newRecord(src, target, i++));
					}
				});
				// we delete records
				associationPersister.delete(associationRecords);
			}
		});
		
		sourcePersister.addDeleteByIdListener(new DeleteByIdListener<SRC>() {
			
			@Override
			public void beforeDeleteById(Iterable<? extends SRC> entities) {
				// We delete association records by entity keys, not their id because we don't have them (it is themselves and we don't have the full
				// entities, only their id)
				// We do it thanks to a SQL delete order ... not very coherent with beforeDelete(..) !
				Delete associationTableDelete = new Delete(associationPersister.getMainTable());
				EntityMapping<SRC, SRCID, T> idMapping = sourcePersister.getMapping();
				Set<SRCID> identifiers = collect(entities, idMapping::getId, HashSet::new);
				if (associationPersister.getMainTable().getOneSideForeignKey().isComposed()) {
					if (dialect.supportsTupleCondition()) {
						// converting ids to tupled-in
						Set<Column> columns = new HashSet<>();
						List<Object[]> values = new ArrayList<>(identifiers.size());
						identifiers.forEach(srcid -> {
							Map<Column<T, ?>, ?> idValues = idMapping.getIdMapping().<T>getIdentifierAssembler().getColumnValues(srcid);
							if (columns.isEmpty()) {	// first time case
								columns.addAll(idValues.keySet());
							}
							values.add(idValues.values().toArray(new Object[0]));
						});
						associationTableDelete.getCriteria().and(new TupleIn(columns.toArray(new Column[0]), values));
					} else {
						throw new UnsupportedOperationException("Can't use tupled-in because database doesn't support it");
					}
				} else {
					Set<Column<T, Object>> columns = associationPersister.getMainTable().getOneSideForeignKey().getColumns();
					associationTableDelete.getCriteria().and(first(columns), Operators.in(identifiers));
				}
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder(associationTableDelete, dialect).toPreparableSQL().toPreparedSQL(new HashMap<>());
				// We don't know how many relations is contained in the table, so we don't check for deletion row count
				try (WriteOperation<Integer> writeOperation = writeOperationFactory.createInstance(deleteStatement, associationPersister.getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
		});
		
		if (getManyRelationDescriptor().isOrphanRemoval()) {
			// adding deletion of many-side entities
			sourcePersister.addDeleteListener(new OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(new OneToManyWithMappedAssociationEngine.DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
		}
	}
	
	public static class TargetInstancesInsertCascader<I, O, J> extends AfterInsertCollectionCascader<I, O> {
		
		private final Accessor<I, ? extends Collection<O>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityReadWriteExecutor<O, J> targetPersister, Accessor<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends O> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<O> getTargets(I source) {
			return collectionGetter.get(source);
		}
	}
	
	protected AfterInsertCollectionCascader<SRC, R> newRecordInsertionCascader(
			Accessor<SRC, C> collectionGetter,
			AssociationRecordPersister<R, T> associationPersister,
			EntityMapping<SRC, SRCID, ?> mappingStrategy,
			EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	protected R newRecord(SRC e, TRGT target, int index) {
		return (R) new AssociationRecord(sourcePersister.getMapping().getId(e), targetPersister.getMapping().getId(target));
	}
}
