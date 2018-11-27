package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.PairIterator;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.OneToManyOptions.RelationshipMaintenanceMode;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.Diff;
import org.gama.stalactite.persistence.id.diff.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operand;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.lang.collection.Iterables.collectToList;
import static org.gama.lang.collection.Iterables.first;
import static org.gama.lang.collection.Iterables.minus;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.reflection.Accessors.of;
import static org.gama.stalactite.persistence.engine.OneToManyOptions.RelationshipMaintenanceMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.OneToManyOptions.RelationshipMaintenanceMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue. {@link CascadeManyConfigurer} leads its management.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<O, Integer>> updatableListIndex = new ThreadLocal<>();
	
	private final ThreadLocal<Map<I, List<AssociationRecord>>> leftAssociations = new ThreadLocal<>();
	private final ThreadLocal<Map<AssociationRecord, O>> rightAssociations = new ThreadLocal<>();
	
	private final IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	private AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister;
	private AssociationRecordPersister<IndexedAssociationRecord, IndexedAssociationTable> indexedAssociationPersister;
	private Column<? extends AssociationTable, Object> pointerToLeftColumn;
	private Dialect dialect;
	private AssociationTable intermediaryTable;
	
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
		for (CascadeType cascadeType : cascadeMany.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
					break;
				case UPDATE:
					addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener, cascadeMany.shouldDeleteRemoved());
					break;
				case DELETE:
					addDeleteCascade(cascadeMany, joinedTablesPersister, leftPersister, collectionGetter, persisterListener, false);
					break;
				case SELECT:
					addSelectCascade(cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn,
							persisterListener);
					break;
			}
		}
		
		RelationshipMaintenanceMode maintenanceMode = cascadeMany.getMaintenanceMode();
		// selection is always present (else configuration is nonsense !)
		addSelectCascade(cascadeMany, joinedTablesPersister, leftPersister, leftPrimaryKey, collectionGetter, rightJoinColumn,
				persisterListener);
		// additionnal cascade
		switch (maintenanceMode) {
			case ALL:
			case ALL_ORPHAN_REMOVAL:
			case ASSOCIATION_ONLY:
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addInsertCascade(cascadeMany, leftPersister, collectionGetter, persisterListener);
				addUpdateCascade(cascadeMany, leftPersister, collectionGetter, persisterListener, maintenanceMode == ALL_ORPHAN_REMOVAL);
				addDeleteCascade(cascadeMany, joinedTablesPersister, leftPersister, collectionGetter, persisterListener, maintenanceMode != ASSOCIATION_ONLY);
				break;
		}
	}
	
	private <T extends Table<T>> void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
													  JoinedTablesPersister<I, J, T> joinedTablesPersister,
													  Persister<O, J, ?> targetPersister,
													  Column leftColumn,
													  Function<I, C> collectionGetter,
													  Column rightColumn,	// can be either the foreign key, or primary key, on the target table
													  PersisterListener<I, J> persisterListener) {
		BeanRelationFixer relationFixer;
		IMutator<I, C> collectionSetter = Accessors.<I, C>of(cascadeMany.getMember()).getMutator();
		// configuring select for fetching relation
		SerializableBiConsumer<O, I> reverseMember = cascadeMany.getReverseSetter();
		if (reverseMember == null) {
			reverseMember = (o, i) -> { /* we can't do anything, so we do ... nothing */ };
		}
		
		if (cascadeMany instanceof CascadeManyList) {
			if (targetPersister.getTargetTable().equals(rightColumn.getTable())
					&& ((CascadeManyList) cascadeMany).getIndexingColumn() != null
			) {
				// we have a direct relation : relationship is owned by target table as a foreign key
				
				// case where right owner and indexing column are defined (on target table) = simple case
				// => we join between tables and add an index capturer
				Column indexingColumn = indexedAssociationPersister == null
						? ((CascadeManyList<I, O, J>) cascadeMany).getIndexingColumn()
						: ((IndexedAssociationTable) intermediaryTable).getIndexColumn();
				relationFixer = addIndexSelection((CascadeManyList<I, O, J>) cascadeMany,
						joinedTablesPersister, targetPersister, (Function<I, List<O>>) collectionGetter, persisterListener,
						(IMutator<I, List<O>>) collectionSetter, reverseMember, indexingColumn);
				joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
						targetPersister,
						relationFixer,
						leftColumn,
						rightColumn,
						true);
			} else {
				// case where no owning column is defined, nor an indexing one : an association table exists (previously defined),
				// we must join on it and add in-memory reassociation
				// Relation is kept on each row by the "relation fixer" passed to JoinedTablePersister because it seems more complex to read it
				// from the Row (as for use case without association table, addTransformerListener(..)) due to the need to create some equivalent
				// structure such as AssociationRecord
				// Relation will be fixed after all rows read (SelectListener.afterSelect)
				addSelectionWithAssociationTable(cascadeMany,
						collectionGetter, persisterListener,
						collectionSetter, reverseMember);
				String joinNodeName = joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
						(Persister<AssociationRecord, AssociationTable, ?>) (Persister) indexedAssociationPersister,
						(BeanRelationFixer<I, AssociationRecord>)
								// implementation to keep track of the relation, further usage is in afterSelect
								(target, input) -> leftAssociations.get().computeIfAbsent(target, k -> new ArrayList<>()).add(input), 
						leftColumn,
						indexedAssociationPersister.getTargetTable().getOneSideKeyColumn(),
						true);
				
				joinedTablesPersister.addPersister(joinNodeName,
						targetPersister,
						(BeanRelationFixer<AssociationRecord, O>)
								// implementation to keep track of the relation, further usage is in afterSelect
								(target, input) -> rightAssociations.get().put(target, input), 
						indexedAssociationPersister.getTargetTable().getManySideKeyColumn(),
						rightColumn,
						true);
			}
		} else {
			if (associationPersister == null) {
				relationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
						cascadeMany.getCollectionTargetClass(), reverseMember);
				
				joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME, targetPersister,
						relationFixer,
						leftColumn, rightColumn, true);
			} else {
				// case where no owning column is defined : an association table exists (previously defined),
				// we must join on it and add in-memory reassociation
				// Relation is kept on each row by the "relation fixer" passed to JoinedTablePersister because it seems more complex to read it
				// from the Row (as for use case without association table, addTransformerListener(..)) due to the need to create some equivalent
				// structure such as AssociationRecord
				// Relation will be fixed after all rows read (SelectListener.afterSelect)
				addSelectionWithAssociationTable(cascadeMany,
						collectionGetter, persisterListener,
						collectionSetter, reverseMember);
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
		}
	}
	
	private BeanRelationFixer addSelectionWithAssociationTable(CascadeMany<I, O, J, C> cascadeMany,
																	Function<I, C> collectionGetter,
																	PersisterListener<I, J> persisterListener,
																	IMutator<I, C> collectionSetter,
																	SerializableBiConsumer<O, I> reverseMember) {
		BeanRelationFixer<I, O> beanRelationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				cascadeMany.getCollectionTargetClass(), reverseMember);
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				leftAssociations.set(new HashMap<>());
				rightAssociations.set(new HashMap<>());
			}
			
			/** Implementation that assembles source and target beans from ThreadLocal elements thanks to the {@link BeanRelationFixer} */
			@Override
			public void afterSelect(Iterable<I> result) {
				try {
					result.forEach(bean -> {
						List<AssociationRecord> associationRecords = leftAssociations.get().get(bean);
						if (associationRecords != null) {
							associationRecords.forEach(s -> beanRelationFixer.apply(bean, rightAssociations.get().get(s)));
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
	
	private <T extends Table<T>> BeanRelationFixer addIndexSelection(CascadeManyList<I, O, J> cascadeMany,
																	 JoinedTablesPersister<I, J, T> joinedTablesPersister,
																	 Persister<O, J, ?> targetPersister,
																	 Function<I, List<O>> collectionGetter,
																	 PersisterListener<I, J> persisterListener,
																	 IMutator<I, List<O>> collectionSetter,
																	 SerializableBiConsumer<O, I> reverseMember,
																	 Column indexingColumn) {
		targetPersister.getSelectExecutor().getMappingStrategy().addSilentColumnSelecter(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
		// May be changed if any performance issue is noticed
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				updatableListIndex.set(new HashMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<I> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(i -> {
						List<O> apply = collectionGetter.apply(i);
						apply.sort(Comparator.comparingInt(o -> updatableListIndex.get().get(o)));
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
				updatableListIndex.remove();
			}
		});
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
			Map<O, Integer> indexPerBean = updatableListIndex.get();
			// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
			// because it was read from ResultSet
			// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
			Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
			indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
		});
		return BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				cascadeMany.getCollectionTargetClass(), reverseMember);
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
	
	private void addUpdateCascade(CascadeMany<I, O, J, C> cascadeMany,
								  Persister<O, J, ?> targetPersister,
								  Function<I, C> collectionGetter,
								  PersisterListener<I, J> persisterListener,
								  boolean shouldDeleteRemoved) {
		BiConsumer<UpdatePayload<I, ?>, Boolean> updateListener;
		if (cascadeMany instanceof CascadeManyList) {
			updateListener = addIndexUpdate((CascadeManyList<I, O, J>) cascadeMany, targetPersister, collectionGetter, shouldDeleteRemoved);
		} else /* any other type of Collection except List */ {
			updateListener = (entry, allColumnsStatement) -> {
				C modified = collectionGetter.apply(entry.getEntities().getLeft());
				C unmodified = collectionGetter.apply(entry.getEntities().getRight());
				Set<Diff> diffSet = differ.diffSet((Set) unmodified, (Set) modified);
				for (Diff diff : diffSet) {
					switch (diff.getState()) {
						case ADDED:
							targetPersister.insert((O) diff.getReplacingInstance());
							break;
						case HELD:
							// NB: update will only be done if necessary by target persister
							targetPersister.update((O) diff.getReplacingInstance(), (O) diff.getSourceInstance(), allColumnsStatement);
							break;
						case REMOVED:
							if (shouldDeleteRemoved) {
								targetPersister.delete((O) diff.getSourceInstance());
							}
							break;
					}
				}
			};
		}
		persisterListener.addUpdateListener(new AfterUpdateCollectionCascader<I, O>(targetPersister) {
			@Override
			public void afterUpdate(Iterable<UpdatePayload<I, ?>> entities, boolean allColumnsStatement) {
				entities.forEach(entry -> updateListener.accept(entry, allColumnsStatement));
			}
			
			@Override
			protected void postTargetUpdate(Iterable<UpdatePayload<O, ?>> entities) {
				// Nothing to do
			}
			
			@Override
			protected Collection<Duo<O, O>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
				throw new NotYetSupportedOperationException();
			}
		});
	}
	
	private BiConsumer<UpdatePayload<I, ?>, Boolean> addIndexUpdate(CascadeManyList<I, O, J> cascadeMany,
																	Persister<O, J, ?> targetPersister,
																	Function<I, C> collectionGetter,
																	boolean shouldDeleteRemoved) {
		BiConsumer<UpdatePayload<I, ?>, Boolean> updateListener;
		updateListener = (updatePayload, allColumnsStatement) -> {
			C modified = collectionGetter.apply(updatePayload.getEntities().getLeft());
			C unmodified = collectionGetter.apply(updatePayload.getEntities().getRight());
			
			// In order to have batch update of the index column (better performance) we compute the whole indexes
			// Then those indexes will be given to the update cascader.
			// But this can only be done through a ThreadLocal (for now) because there's no way to give them directly
			// Hence we need to be carefull of Thread safety (cleaning context and collision)
			
			Set<IndexedDiff> diffSet = differ.diffList((List) unmodified, (List) modified);
			// a List to keep SQL orders, for better debug, easier understanding of logs
			List<O> toBeInserted = new ArrayList<>();
			List<O> toBeDeleted = new ArrayList<>();
			List<IndexedAssociationRecord> indexedAssociationRecordstoBeInserted = new ArrayList<>();
			List<IndexedAssociationRecord> indexedAssociationRecordstoBeDeleted = new ArrayList<>();
			List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> indexedAssociationRecordstoBeUpdated = new ArrayList<>();
			Map<O, Integer> newIndexes = new HashMap<>();
			for (IndexedDiff diff : diffSet) {
				switch (diff.getState()) {
					case ADDED:
						if (indexedAssociationPersister != null) {
							diff.getReplacerIndexes().forEach(idx ->
									indexedAssociationRecordstoBeInserted.add(new IndexedAssociationRecord(updatePayload.getEntities().getLeft().getId(), diff.getReplacingInstance().getId(), idx)));
						}
						// we insert only non persisted entity to prevent from a primary key conflict
						if (!diff.getReplacingInstance().getId().isPersisted()) {
							toBeInserted.add((O) diff.getReplacingInstance());
						}
						break;
					case HELD:
						Set<Integer> minus = minus(diff.getReplacerIndexes(), diff.getSourceIndexes());
						Integer index = first(minus);
						if (index != null ) {
							newIndexes.put((O) diff.getReplacingInstance(), index);
							if (indexedAssociationPersister != null) {
								PairIterator<Integer, Integer> diffIndexIterator = new PairIterator<>(diff.getReplacerIndexes(),
										diff.getSourceIndexes());
								diffIndexIterator.forEachRemaining(d -> {
									if (!d.getLeft().equals(d.getRight()))
										indexedAssociationRecordstoBeUpdated.add(new Duo<>(
												new IndexedAssociationRecord(updatePayload.getEntities().getLeft().getId(), diff.getSourceInstance().getId(), d.getLeft()),
												new IndexedAssociationRecord(updatePayload.getEntities().getLeft().getId(), diff.getSourceInstance().getId(), d.getRight())));
								});
							} else {
								targetPersister.getMappingStrategy().addSilentColumnUpdater(cascadeMany.getIndexingColumn(),
										// Thread safe by updatableListIndex access
										(Function<O, Object>) o -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(o)).get());
							}
						}
						break;
					case REMOVED:
						if (indexedAssociationPersister != null) {
							diff.getSourceIndexes().forEach(idx ->
									indexedAssociationRecordstoBeDeleted.add(new IndexedAssociationRecord(updatePayload.getEntities().getLeft().getId(), diff.getSourceInstance().getId(), idx)));
						}
						// we delete only persisted entity to prevent from a not found record
						if (shouldDeleteRemoved && diff.getSourceInstance().getId().isPersisted()) {
							toBeDeleted.add((O) diff.getSourceInstance());
						}
						break;
				}
			}
			// we batch index update
			ThreadLocals.doWithThreadLocal(updatableListIndex, () -> newIndexes, (Runnable) () -> {
				List<Duo<O, O>> collect = collectToList(updatableListIndex.get().keySet(), o -> new Duo<>(o, o));
				targetPersister.update(collect, false);
			});
			// we batch added and deleted objects
			if (indexedAssociationPersister != null) {
				indexedAssociationPersister.delete(indexedAssociationRecordstoBeDeleted);
			}
			targetPersister.insert(toBeInserted);
			targetPersister.delete(toBeDeleted);
			if (indexedAssociationPersister != null) {
				indexedAssociationPersister.insert(indexedAssociationRecordstoBeInserted);
				// we ask for index update : all columns shouldn't be updated, only index, so we don't need "all columns in statement"
				indexedAssociationPersister.update(indexedAssociationRecordstoBeUpdated, false);
			}
		};
		return updateListener;
	}
	
	public void addInsertCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener) {
		// For a List and a given manner to get its owner (so we can deduce index value), we configure persistence to keep index value in database
		if (cascadeMany instanceof CascadeManyList && cascadeMany.getReverseGetter() != null) {
			addIndexInsertion((CascadeManyList<I, O, J>) cascadeMany, targetPersister);
		}
		persisterListener.addInsertListener(new AfterInsertCollectionCascader<I, O>(targetPersister) {
			
			@Override
			protected void postTargetInsert(Iterable<O> entities) {
				// Nothing to do. Identified#isPersisted flag should be fixed by target persister
			}
			
			@Override
			protected Collection<O> getTargets(I o) {
				Collection<O> targets = collectionGetter.apply(o);
				// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
				return stream(targets)
						.filter(CascadeOneConfigurer.NON_PERSISTED_PREDICATE)
						.collect(Collectors.toList());
			}
		});
		
		if (associationPersister != null) {
			persisterListener.addInsertListener(new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter));
		}
		
		if (indexedAssociationPersister != null) {
			persisterListener.addInsertListener(new IndexedAssociationRecordInsertionCascader<>(indexedAssociationPersister, (Function<I, List<O>>) collectionGetter));
		}
	}
	
	/**
	 * Adds a "listener" that will amend insertion of the index column filled with its value
	 * @param cascadeMany
	 * @param targetPersister
	 */
	private void addIndexInsertion(CascadeManyList<I, O, J> cascadeMany, Persister<O, J, ?> targetPersister) {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(cascadeMany.getIndexingColumn(),
				(Function<O, Object>) target -> {
					I source = cascadeMany.getReverseGetter().apply(target);
					if (source == null) {
						MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
						Method method = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
						throw new IllegalStateException("Impossible to get index : " + target + " is not associated with a " + Reflections.toString(method.getReturnType()) + " : "
							+ Reflections.toString(method) + " returned null");
					}
					List<O> collection = cascadeMany.getTargetProvider().apply(source);
					return collection.indexOf(target);
				});
	}
}
