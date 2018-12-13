package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithIndexedAssociationTableEngine;
import org.gama.stalactite.persistence.engine.runtime.OneToManyWithMappedAssociationEngine;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operand;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.lang.collection.Iterables.first;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.reflection.Accessors.of;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.READ_ONLY;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	private AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister;
	private AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> indexedAssociationPersister;
	private Column<? extends AssociationTable, Object> pointerToLeftColumn;
	private Dialect dialect;
	private AssociationTable intermediaryTable;
	/** Setter for applying source entity to reverse side (target entity). Available only when association is mapped without intermediary table */
	private BiConsumer<O, I> reverseSetter = null;
	
	public <T extends Table<T>> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
												   JoinedTablesPersister<I, J, T> joinedTablesPersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
												   AssociationTableNamingStrategy associationTableNamingStrategy, Dialect dialect) {
		this.dialect = dialect;
		Persister<O, J, ?> leftPersister = cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((InsertListener<I>) SetPersistedFlagAfterInsertListener.INSTANCE);
		leftPersister.getPersisterListener().addInsertListener((InsertListener<O>) SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		if (joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
		Column leftPrimaryKey = first(joinedTablesPersister.getTargetTable().getPrimaryKey().getColumns());
		
		Column rightJoinColumn;
		if (cascadeMany.getReverseSetter() == null && cascadeMany.getReverseGetter() == null && cascadeMany.getReverseColumn() == null) {
			// case : Collection mapping without reverse property : an association table is needed
			Table<?> rightTable = leftPersister.getMappingStrategy().getTargetTable();
			Column rightPrimaryKey = first(rightTable.getPrimaryKey().getColumns());
			
			String associationTableName = associationTableNamingStrategy.giveName(cascadeMany.getMember(), leftPrimaryKey, rightPrimaryKey);
			if (cascadeMany instanceof CascadeManyList) {
				
				if (((CascadeManyList) cascadeMany).getIndexingColumn() != null) {
					throw new UnsupportedOperationException("Indexing column id defined but not owner is defined : relation is only mapped by "
							+ Reflections.toString(cascadeMany.getMember()));
				}
				
				// NB: index column is part of the primary key
				intermediaryTable = new IndexedAssociationTable(null, associationTableName, leftPrimaryKey, rightPrimaryKey,
						associationTableNamingStrategy, foreignKeyNamingStrategy);
				pointerToLeftColumn = intermediaryTable.getOneSideKeyColumn();
				indexedAssociationPersister = new AssociationRecordPersister<>(
						new IndexedAssociationRecordMappingStrategy((IndexedAssociationTable) intermediaryTable),
						dialect,
						joinedTablesPersister.getConnectionProvider(),
						joinedTablesPersister.getBatchSize());
			} else {
				intermediaryTable = new AssociationTable(null, associationTableName, leftPrimaryKey, rightPrimaryKey, associationTableNamingStrategy,
						foreignKeyNamingStrategy);
				pointerToLeftColumn = intermediaryTable.getOneSideKeyColumn();
				associationPersister = new AssociationRecordPersister<>(
						new AssociationRecordMappingStrategy(intermediaryTable),
						dialect, 
						joinedTablesPersister.getConnectionProvider(),
						joinedTablesPersister.getBatchSize());
			}
			
			// for further select usage, see far below
			rightJoinColumn = rightPrimaryKey;
		} else {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			// We're looking for the foreign key (no association table needed)
			// Determining right side column
			Column foreignKey = cascadeMany.getReverseColumn();
			
			Method reverseMember = null;
			if (foreignKey == null) {
				// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
				MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
				if (cascadeMany.getReverseSetter() != null) {
					reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseSetter());
				} else {
					reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
				}
				reverseSetter = of(reverseMember)::set;
				foreignKey = leftPersister.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(of(reverseMember));
				if (foreignKey == null) {
					// This should not happen, left for bug safety
					throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
							+ Reflections.toString(joinedTablesPersister.getMappingStrategy().getClassToPersist())
							+ " to persister of " + cascadeMany.getPersister().getMappingStrategy().getClassToPersist().getName());
				}
			}
			
			if (cascadeMany instanceof CascadeManyList && ((CascadeManyList) cascadeMany).getIndexingColumn() == null) {
				throw new UnsupportedOperationException("Missing indexing column : relation is mapped by "
						+ (reverseMember != null ? Reflections.toString(reverseMember) : cascadeMany.getReverseColumn())
						+ " but no indexing property is defined");
			}
			
			// adding foreign key constraint
			foreignKey.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(foreignKey, leftPrimaryKey), foreignKey, leftPrimaryKey);
			
			// for further select usage, see far below
			rightJoinColumn = foreignKey;
		}
		
		// managing cascades
		Function<I, C> collectionGetter = cascadeMany.getTargetProvider();
		PersisterListener<I, J> persisterListener = joinedTablesPersister.getPersisterListener();
		
		RelationshipMode maintenanceMode = cascadeMany.getRelationshipMode();
		// selection is always present (else configuration is nonsense !)
		if (associationPersister != null) {
			OneToManyWithAssociationTableEngine<I, O, J, C>  oneToManyWithAssociationTableEngine = new OneToManyWithAssociationTableEngine(
					associationPersister);
			oneToManyWithAssociationTableEngine.addSelectCascade(
					cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				oneToManyWithAssociationTableEngine.addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
				oneToManyWithAssociationTableEngine.addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener, maintenanceMode == ALL_ORPHAN_REMOVAL);
			}
		}
		if (indexedAssociationPersister != null) {
			OneToManyWithIndexedAssociationTableEngine<I, O, J, List<O>>  oneToManyWithIndexedAssociationTableEngine = new OneToManyWithIndexedAssociationTableEngine(
					indexedAssociationPersister);
			oneToManyWithIndexedAssociationTableEngine.addSelectCascade(
					(CascadeManyList) cascadeMany,
					joinedTablesPersister, leftPersister, leftPrimaryKey, (Function) collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				oneToManyWithIndexedAssociationTableEngine.addInsertCascade(leftPersister, (Function) collectionGetter, persisterListener);
				oneToManyWithIndexedAssociationTableEngine.addUpdateCascade((CascadeManyList) cascadeMany, leftPersister, (Function) collectionGetter, persisterListener, maintenanceMode == ALL_ORPHAN_REMOVAL);
			}
		}
		if (associationPersister == null && indexedAssociationPersister == null) {
			// we have a direct relation : relationship is owned by target table as a foreign key
			OneToManyWithMappedAssociationEngine mappedAssociationEngine = new OneToManyWithMappedAssociationEngine(reverseSetter);
			mappedAssociationEngine.addSelectCascade(cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn, persisterListener);
			if (maintenanceMode != READ_ONLY) {
				mappedAssociationEngine.addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
				mappedAssociationEngine.addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener, maintenanceMode == ALL_ORPHAN_REMOVAL);
			}
		}
		// additionnal cascade
		switch (maintenanceMode) {
			case ASSOCIATION_ONLY:
				if (intermediaryTable == null) {
					throw new MappingConfigurationException(RelationshipMode.ASSOCIATION_ONLY + " is only relevent with an association table");
				}
			case ALL:
			case ALL_ORPHAN_REMOVAL:
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addDeleteCascade(cascadeMany, joinedTablesPersister, leftPersister, collectionGetter, persisterListener, maintenanceMode != ASSOCIATION_ONLY);
				break;
		}
	}
	
	private <T extends Table<T>> void addDeleteCascade(CascadeMany<I, O, J, C> cascadeMany,
													   JoinedTablesPersister<I, J, T> joinedTablesPersister,
													   Persister<O, J, ?> targetPersister,
													   Function<I, C> collectionGetter,
													   PersisterListener<I, J> persisterListener,
													   boolean deleteTargetEntities) {
		// we delete association records
		if (indexedAssociationPersister != null) {
			persisterListener.addDeleteListener(new DeleteListener<I>() {
				@Override
				public void beforeDelete(Iterable<I> entities) {
					// We delete the association records by their ids (that are ... themselves) 
					// We could have deleted them with a delete order but this requires a binder registry which is given by a Dialect
					// so it requires that this configurer holds the Dialect which is not the case, but could have.
					// It should be more efficient because, here, we have to create as many AssociationRecord as necessary which loads the garbage collector
					List<IndexedAssociationRecord> associationRecords = new ArrayList<>();
					entities.forEach(e -> {
						Collection<O> targets = collectionGetter.apply(e);
						int i = 0;
						for (O target : targets) {
							associationRecords.add(new IndexedAssociationRecord(e.getId(), target.getId(), i++));
						}
					});
					indexedAssociationPersister.deleteById(associationRecords);
				}
			});
			
			persisterListener.addDeleteByIdListener(new DeleteByIdListener<I>() {
				
				@Override
				public void beforeDeleteById(Iterable<I> entities) {
					// We delete records thanks to delete order
					// Yes it is no coherent with beforeDelete(..) !
					Delete<IndexedAssociationTable> delete = new Delete<>(indexedAssociationPersister.getTargetTable());
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
		} else if (associationPersister != null) {
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
		}
		// NB: with such a else, target entities are deleted only when no association table exists
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
