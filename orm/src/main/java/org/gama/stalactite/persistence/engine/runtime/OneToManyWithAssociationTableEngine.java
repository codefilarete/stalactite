package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operand;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	private final ThreadLocal<Map<I, List<AssociationRecord>>> leftAssociations = new ThreadLocal<>();
	private final ThreadLocal<Map<AssociationRecord, O>> rightAssociations = new ThreadLocal<>();
	
	private final AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister;
	
	public OneToManyWithAssociationTableEngine(AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		this.associationPersister = associationPersister;
	}
	
	public <T extends Table<T>> void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
													   JoinedTablesPersister<I, J, T> joinedTablesPersister,
													   Persister<O, J, ?> targetPersister,
													   Column leftColumn,
													   Function<I, C> collectionGetter,
													   Column rightColumn,    // can be either the foreign key, or primary key, on the target table
													   PersisterListener<I, J> persisterListener) {
		
		IMutator<I, C> collectionSetter = Accessors.<I, C>of(cascadeMany.getMember()).getMutator();
		
		// case where no owning column is defined : an association table exists (previously defined),
		// we must join on it and add in-memory reassociation
		// Relation is kept on each row by the "relation fixer" passed to JoinedTablePersister because it seems more complex to read it
		// from the Row (as for use case without association table, addTransformerListener(..)) due to the need to create some equivalent
		// structure such as AssociationRecord
		// Relation will be fixed after all rows read (SelectListener.afterSelect)
		addSelectionWithAssociationTable(
				cascadeMany.getCollectionTargetClass(), collectionGetter, persisterListener,
				collectionSetter);
		String joinNodeName = joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
				(Persister<AssociationRecord, AssociationTable, ?>) (Persister) associationPersister,
				(BeanRelationFixer<I, AssociationRecord>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(target, input) -> leftAssociations.get().computeIfAbsent(target, k -> new ArrayList<>()).add(input),
				leftColumn,
				associationPersister.getTargetTable().getOneSideKeyColumn(),
				true);
		
		joinedTablesPersister.addPersister(joinNodeName,
				targetPersister,
				(BeanRelationFixer<AssociationRecord, O>)
						// implementation to keep track of the relation, further usage is in afterSelect
						(target, input) -> rightAssociations.get().put(target, input),
				associationPersister.getTargetTable().getManySideKeyColumn(),
				rightColumn,
				true);
	}
	
	private BeanRelationFixer addSelectionWithAssociationTable(Class<C> collectionTargetClass,
															   Function<I, C> collectionGetter,
															   PersisterListener<I, J> persisterListener,
															   IMutator<I, C> collectionSetter) {
		BeanRelationFixer<I, O> beanRelationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				collectionTargetClass, OneToManyWithMappedAssociationEngine.NOOP_REVERSE_SETTER);
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				leftAssociations.set(new HashMap<>());
				rightAssociations.set(new HashMap<>());
			}
			
			/** Implementation that assembles source and target beans from ThreadLocal elements thanks to the {@link BeanRelationFixer}
			 * @param result*/
			@Override
			public void afterSelect(Iterable<? extends I> result) {
				try {
					result.forEach(bean -> {
						List<AssociationRecord> associationRecords = leftAssociations.get().get(bean);
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
		return beanRelationFixer;
	}
	
	public void addInsertCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener) {
		persisterListener.addInsertListener(new TargetInstancesInsertCascader<>(targetPersister, collectionGetter));
		
		persisterListener.addInsertListener(new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter));
	}
	
	public void addUpdateCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener,
								 boolean shouldDeleteRemoved) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevent
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<I, O, C> updateListener = new CollectionUpdater<I, O, C>(collectionGetter, targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onAddedTarget(UpdateContext updateContext, AbstractDiff diff) {
				super.onAddedTarget(updateContext, diff);
				AssociationRecord associationRecord = new AssociationRecord(updateContext.getPayload().getEntities().getLeft().getId(),
						diff.getReplacingInstance().getId());
				((AssociationTableUpdateContext) updateContext).getAssociationRecordstoBeInserted().add(associationRecord);
			}
			
			@Override
			protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff diff) {
				super.onRemovedTarget(updateContext, diff);
				
				AssociationRecord associationRecord = new AssociationRecord(updateContext.getPayload().getEntities().getLeft().getId(),
						diff.getSourceInstance().getId());
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
				
				private final List<AssociationRecord> associationRecordstoBeInserted = new ArrayList<>();
				private final List<AssociationRecord> associationRecordstoBeDeleted = new ArrayList<>();
				
				public AssociationTableUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
					super(updatePayload);
				}
				
				public List<AssociationRecord> getAssociationRecordstoBeInserted() {
					return associationRecordstoBeInserted;
				}
				
				public List<AssociationRecord> getAssociationRecordstoBeDeleted() {
					return associationRecordstoBeDeleted;
				}
			}
		};
		
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public <T extends Table<T>> void addDeleteCascade(CascadeMany<I, O, J, C> cascadeMany,
													  JoinedTablesPersister<I, J, T> joinedTablesPersister,
													  Persister<O, J, ?> targetPersister,
													  Function<I, C> collectionGetter,
													  PersisterListener<I, J> persisterListener,
													  boolean deleteTargetEntities,
													  Dialect dialect,
													  Column<? extends AssociationTable, Object> pointerToLeftColumn) {
		// we delete association records
		persisterListener.addDeleteListener(new DeleteListener<I>() {
			@Override
			public void beforeDelete(Iterable<I> entities) {
				// We delete the association records by their ids (that are ... themselves) 
				// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
				// so it requires that this configurer holds the Dialect which is not the case, but could have.
				// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
				List<AssociationRecord> associationRecords = new ArrayList<>();
				entities.forEach(e -> {
					Collection<O> targets = collectionGetter.apply(e);
					for (O target : targets) {
						associationRecords.add(new AssociationRecord(e.getId(), target.getId()));
					}
				});
				associationPersister.delete(associationRecords);
			}
		});
		
		persisterListener.addDeleteByIdListener(new DeleteByIdListener<I>() {
			
			@Override
			public void beforeDeleteById(Iterable<I> entities) {
				// We delete records thanks to delete order
				// Yes it is no coherent with beforeDelete(..) !
				Delete<AssociationTable> delete = new Delete<>(associationPersister.getTargetTable());
				ClassMappingStrategy<I, J, T> mappingStrategy = joinedTablesPersister.getMappingStrategy();
				List<J> identifiers = collect(entities, mappingStrategy::getId, ArrayList::new);
				delete.where(pointerToLeftColumn, Operand.in(identifiers));
				
				PreparedSQL deleteStatement = new DeleteCommandBuilder<>(delete).toStatement(dialect.getColumnBinderRegistry());
				try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, cascadeMany.getPersister().getConnectionProvider())) {
					writeOperation.setValues(deleteStatement.getValues());
					writeOperation.execute();
				}
			}
		});
		
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, O>(targetPersister) {
				
				@Override
				protected void postTargetDelete(Iterable<O> entities) {
					// no post treatment to do
				}
				
				@Override
				protected Collection<O> getTargets(I i) {
					Collection<O> targets = collectionGetter.apply(i);
					// We only delete persisted instances (for logic and to prevent from non matching row count exception)
					return stream(targets)
							.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
							.collect(Collectors.toList());
				}
			});
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			persisterListener.addDeleteByIdListener(new BeforeDeleteByIdCollectionCascader<I, O>(targetPersister) {
				@Override
				protected void postTargetDelete(Iterable<O> entities) {
					// no post treatment to do
				}
				
				@Override
				protected Collection<O> getTargets(I i) {
					Collection<O> targets = collectionGetter.apply(i);
					// We only delete persisted instances (for logic and to prevent from non matching row count exception)
					return stream(targets)
							.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
							.collect(Collectors.toList());
				}
			});
		}
	}
}
