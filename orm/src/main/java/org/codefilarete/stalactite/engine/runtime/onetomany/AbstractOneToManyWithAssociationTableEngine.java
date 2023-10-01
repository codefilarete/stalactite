package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteByIdTargetEntitiesBeforeDeleteByIdCascader;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.collect;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, R extends AssociationRecord, T extends AssociationTable<T, ?, ?, SRCID, TRGTID>> {
	
	protected final AssociationRecordPersister<R, T> associationPersister;
	
	protected final PersisterListenerCollection<SRC, SRCID> persisterListener;
	
	protected final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	
	protected final ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	private final WriteOperationFactory writeOperationFactory;
	
	public AbstractOneToManyWithAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
													   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
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
	
	public void addSelectCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister, boolean loadSeparately) {
		
		// we join on the association table and add bean association in memory
		String associationTableJoinNodeName = sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSideKey(),
				associationPersister.getMainTable().getOneSideForeignKey(),
				JoinType.OUTER, Collections.emptySet());
		
		// we add target subgraph joins to main persister
		targetPersister.joinAsMany(sourcePersister, associationPersister.getMainTable().getManySideForeignKey(),
				associationPersister.getMainTable().getManySideKey(), manyRelationDescriptor.getRelationFixer(),
				null, associationTableJoinNodeName, true, loadSeparately);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
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
	}
	
	public void addInsertCascade(boolean maintainAssociationOnly) {
		// Can we cascade insert on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addInsertListener(new TargetInstancesInsertCascader(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		}
		
		persisterListener.addInsertListener(newRecordInsertionCascader(
				manyRelationDescriptor.getCollectionGetter(),
				associationPersister,
				sourcePersister.getMapping(),
				targetPersister.getMapping()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved, boolean maintainAssociationOnly) {
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevant
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, C> collectionUpdater = new CollectionUpdater<SRC, TRGT, C>(manyRelationDescriptor.getCollectionGetter(), targetPersister, null, shouldDeleteRemoved) {
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
		if (!maintainAssociationOnly) {
			persisterListener.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
		}
	}
	
	/**
	 * Add deletion of association records on {@link BeanPersister#delete} and {@link BeanPersister#deleteById} events.
	 * If {@code deleteTargetEntities} is true, then will also delete target (many side) entities.
	 * 
	 * In case of {@link BeanPersister#deleteById}, association records will be deleted only by source entity keys.
	 * 
	 * @param deleteTargetEntities true to delete many-side entities, false to only delete association records
	 * @param dialect necessary to build valid SQL for deleteById action
	 */
	public void addDeleteCascade(boolean deleteTargetEntities, Dialect dialect) {
		// we delete association records
		persisterListener.addDeleteListener(new DeleteListener<SRC>() {
			@Override
			public void beforeDelete(Iterable<? extends SRC> entities) {
				// To be coherent with DeleteListener, we'll delete the association records by ... themselves, not by id.
				// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
				// so it requires that this configurer holds the Dialect which is not the case, but could have.
				// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
				List<R> associationRecords = new ArrayList<>();
				entities.forEach(src -> {
					Collection<TRGT> targets = nullable(manyRelationDescriptor.getCollectionGetter().apply(src)).getOr(manyRelationDescriptor.getCollectionFactory());
					int i = 0;
					for (TRGT target : targets) {
						associationRecords.add(newRecord(src, target, i++));
					}
				});
				// we delete records
				associationPersister.delete(associationRecords);
			}
		});
		
		persisterListener.addDeleteByIdListener(new DeleteByIdListener<SRC>() {
			
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
							Map<Column<T, Object>, Object> idValues = idMapping.getIdMapping().<T>getIdentifierAssembler().getColumnValues(srcid);
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
					associationTableDelete.where(first(associationPersister.getMainTable().getOneSideForeignKey().getColumns()), Operators.in(identifiers));
				}
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder(associationTableDelete, dialect).toStatement();
				// We don't know how many relations is contained in the table, so we don't check for deletion row count
				try (WriteOperation<Integer> writeOperation = writeOperationFactory.createInstance(deleteStatement, associationPersister.getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
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
																						EntityMapping<SRC, SRCID, ?> mappingStrategy,
																						EntityMapping<TRGT, TRGTID, ?> strategy);
	
	protected abstract R newRecord(SRC e, TRGT target, int index);
	
	public void addSelectCascadeIn2Phases(FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		// we join on the association table and add bean association in memory
		IdentifierAssembler<TRGTID, ?> targetIdentifierAssembler = targetPersister.getMapping().getIdMapping().getIdentifierAssembler();
		sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSideKey(),	// could be taken on source persister, but it's same thing
				associationPersister.getMainTable().getOneSideForeignKey(),
				JoinType.OUTER,
				(Set) associationPersister.getMainTable().getManySideForeignKey().getColumns(),
				(src, columnValueProvider) -> {
					// we take TRGTID from targetPersister id assembler which read it from right table primary key but
					// it is not in join : only association table is. So we wrap column value provider in one that
					// get association table column that matches the one asked by targetPersister thanks to association table
					// FK-PK columns mapping
					Function<Column<?, ?>, Object> manySideColumnValueProvider = columnFromRightTablePK -> {
						// getting column present in query through association table FK-PK mapping
						Column<T, Object> columnFromAssociationTableFK = associationPersister.getMainTable().getRightIdentifierColumnMapping().get(columnFromRightTablePK);
						if (columnFromAssociationTableFK == null) {
							throw new IllegalStateException("No matching column in foreign key of association table " + associationPersister.getMainTable() + " found for primary key " + columnFromRightTablePK);
						}
						return columnValueProvider.apply(columnFromAssociationTableFK);
					};
					firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, targetIdentifierAssembler.assemble(manySideColumnValueProvider));
				});
	}
	
	public static class TargetInstancesInsertCascader<I, O, J> extends AfterInsertCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<O, J> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends O> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<O> getTargets(I source) {
			return collectionGetter.apply(source);
		}
	}
}
