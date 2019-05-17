package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.function.Predicates.not;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.READ_ONLY;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <ID> identifier type of target entities
 * @author Guillaume Mary
 */
public class CascadeOneConfigurer<SRC, TRGT, ID> {
	
	private final PersistenceContext persistenceContext;
	
	public CascadeOneConfigurer(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
	}
	
	public <T extends Table<T>> void appendCascade(
			CascadeOne<SRC, TRGT, ID> cascadeOne,
			JoinedTablesPersister<SRC, ID, T> sourcePersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		
		if (cascadeOne.getReverseGetter() == null && cascadeOne.getReverseSetter() == null && cascadeOne.getReverseColumn() == null) {
			new RelationOwnedBySourceConfigurer<SRC, TRGT, ID>(persistenceContext).appendCascade(cascadeOne, sourcePersister, foreignKeyNamingStrategy);
		} else {
			new RelationOwnedByTargetConfigurer<SRC, TRGT, ID>(persistenceContext).appendCascade(cascadeOne, sourcePersister, foreignKeyNamingStrategy);
		}
	}
	
	private abstract static class ConfigurerTemplate<SRC, TRGT, ID> {
		
		private final PersistenceContext persistenceContext;
		
		ConfigurerTemplate(PersistenceContext persistenceContext) {
			this.persistenceContext = persistenceContext;
		}
		
		<T extends Table<T>> void appendCascade(
				CascadeOne<SRC, TRGT, ID> cascadeOne,
				JoinedTablesPersister<SRC, ID, T> joinedTablesPersister,
				ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			
			RelationshipMode maintenanceMode = cascadeOne.getRelationshipMode();
			if (maintenanceMode == ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(ASSOCIATION_ONLY + " is only relevent for one-to-many association");
			}
			ClassMappingStrategy<SRC, ID, T> mappingStrategy = joinedTablesPersister.getMappingStrategy();
			if (mappingStrategy.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
				throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
			}
			
			IReversibleAccessor<SRC, TRGT> targetAccessor = cascadeOne.getTargetProvider();
			
			EntityMappingConfiguration<TRGT, ID> targetMappingConfiguration = cascadeOne.getTargetMappingConfiguration();
			
			Persister<TRGT, ID, Table> targetPersister = new EntityMappingBuilder<>(targetMappingConfiguration, new MethodReferenceCapturer())
					.build(persistenceContext, Nullable.nullable(cascadeOne.getTargetTable()).orGet(Nullable.nullable(cascadeOne.getReverseColumn()).apply(Column::getTable).get()));
//					.build(persistenceContext, Nullable.nullable(cascadeOne.getReverseColumn()).apply(Column::getTable).get());
			ClassMappingStrategy<TRGT, ID, Table> targetMappingStrategy = targetPersister.getMappingStrategy();
			
			// Finding joined columns
			Duo<Column, Column> foreignKeyColumns = determineForeignKeyColumns(cascadeOne, mappingStrategy, targetAccessor,
					targetMappingStrategy, foreignKeyNamingStrategy);
			
			Column leftColumn = foreignKeyColumns.getLeft();
			Column rightColumn = foreignKeyColumns.getRight();
			
			// selection is always present (else configuration is nonsense !)
			BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer(targetAccessor);
			addSelectCascade(cascadeOne, joinedTablesPersister, targetPersister, leftColumn, rightColumn, beanRelationFixer);
			
			// additionnal cascade
			PersisterListener<SRC, ID> srcPersisterListener = joinedTablesPersister.getPersisterListener();
			boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
			boolean writeAuthorized = maintenanceMode != READ_ONLY;
			if (writeAuthorized) {
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
				addUpdateCascade(cascadeOne, targetPersister, srcPersisterListener, orphanRemoval);
				addDeleteCascade(cascadeOne, targetPersister, srcPersisterListener, orphanRemoval);
			}
		}
		
		protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(IReversibleAccessor<SRC, TRGT> targetAccessor) {
			IMutator<SRC, TRGT> targetSetter = targetAccessor.toMutator();
			return BeanRelationFixer.of(targetSetter::set);
		}
		
		protected abstract <T extends Table<T>> Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																							   ClassMappingStrategy<SRC, ID, T> mappingStrategy,
																							   IReversibleAccessor<SRC, TRGT> targetAccessor,
																							   ClassMappingStrategy<TRGT, ID, Table> targetMappingStrategy,
																							   ForeignKeyNamingStrategy foreignKeyNamingStrategy);
		
		protected void addInsertCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
												 Persister<TRGT, ID, Table> targetPersister,
												 PersisterListener<SRC, ID> srcPersisterListener) {
			// adding persistence flag setters on other side : this could be done by Persister itself,
			// but we would loose the reason why it does it : the cascade functionnality
			targetPersister.getPersisterListener().addInsertListener(
					targetPersister.getMappingStrategy().getIdMappingStrategy().getIdentifierInsertionManager().getInsertListener());
			
		}
		
		protected abstract void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne, Persister<TRGT, ID, Table> targetPersister,
												 PersisterListener<SRC, ID> srcPersisterListener, boolean orphanRemoval);
		
		protected abstract void addDeleteCascade(CascadeOne<SRC, TRGT, ID> cascadeOne, Persister<TRGT, ID, Table> targetPersister,
												 PersisterListener<SRC, ID> srcPersisterListener, boolean orphanRemoval);
		
		protected <T extends Table<T>> void addSelectCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
															 JoinedTablesPersister<SRC, ID, T> joinedTablesPersister,
															 Persister<TRGT, ID, Table> targetPersister,
															 Column leftColumn,
															 Column rightColumn,
															 BeanRelationFixer<SRC, TRGT> beanRelationFixer) {
			// configuring select for fetching relation
			joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
					beanRelationFixer,
					leftColumn, rightColumn, cascadeOne.isNullable());
		}
		
	}
	
	private static class RelationOwnedBySourceConfigurer<SRC, TRGT, ID> extends ConfigurerTemplate<SRC, TRGT, ID> {
		
		private RelationOwnedBySourceConfigurer(PersistenceContext persistenceContext) {
			super(persistenceContext);
		}
		
		@Override
		protected <T extends Table<T>> Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																					  ClassMappingStrategy<SRC, ID, T> mappingStrategy,
																					  IReversibleAccessor<SRC, TRGT> targetAccessor,
																					  ClassMappingStrategy<TRGT, ID, Table> targetMappingStrategy,
																					  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			// targetAccessor may not be the one that was declared is propertyToColumn, so we need to wrap them into a more flexible search structure : ValueAccessPointMap
			Column<T, ID> leftColumn = (Column<T, ID>) new ValueAccessPointMap<Column>(mappingStrategy.getMainMappingStrategy().getPropertyToColumn()).get(targetAccessor);
			// According to the nullable option, we specify the ddl schema option
			leftColumn.nullable(cascadeOne.isNullable());
			Column<?, ID> rightColumn = (Column<?, ID>) Iterables.first((Set<Column<?, Object>>) targetMappingStrategy.getTargetTable().getPrimaryKey().getColumns());
			
			// adding foreign key constraint
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftColumn, rightColumn);
			leftColumn.getTable().addForeignKey(foreignKeyName, leftColumn, rightColumn);
			return new Duo<>(leftColumn, rightColumn);
		}
		
		@Override
		protected void addInsertCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
									 Persister<TRGT, ID, Table> targetPersister,
									 PersisterListener<SRC, ID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addInsertListener(new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider()));
			}
			// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
			Predicate<TRGT> insertionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(targetPersister.getMappingStrategy().getIdMappingStrategy()::isNew);
			srcPersisterListener.addInsertListener(new BeforeInsertSupport<>(targetPersister::insert, cascadeOne.getTargetProvider()::get, insertionPredicate));
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
										Persister<TRGT, ID, Table> targetPersister,
										PersisterListener<SRC, ID> srcPersisterListener,
										boolean orphanRemoval) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addUpdateListener(new MandatoryRelationCheckingBeforeUpdateListener<>(cascadeOne.getTargetProvider()));
			}
			// adding cascade treatment
			// - insert non-persisted target instances to fulfill foreign key requirement
			srcPersisterListener.addUpdateListener(new BeforeUpdateSupport<>(
					// we insert new instances
					(it, b) -> targetPersister.insert(Iterables.collectToList(it, Duo::getLeft)),
					cascadeOne.getTargetProvider()::get,
					// we only keep targets of modified instances, non null and not yet persisted
					Predicates.predicate(Duo::getLeft, Predicates.<TRGT>predicate(Objects::nonNull).and(targetPersister.getMappingStrategy()::isNew))
			));
			// - after source update, target is updated too
			srcPersisterListener.addUpdateListener(new UpdateListener<SRC>() {
				
				@Override
				public void afterUpdate(Iterable<UpdatePayload<? extends SRC, ?>> payloads, boolean allColumnsStatement) {
					List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
							// targets of nullified relations don't need to be updated 
							e -> getTarget(e.getEntities().getLeft()) != null,
							e -> getTargets(e.getEntities().getLeft(), e.getEntities().getRight()),
							ArrayList::new);
					targetPersister.update(targetsToUpdate, allColumnsStatement);
				}
				
				private Duo<TRGT, TRGT> getTargets(SRC modifiedTrigger, SRC unmodifiedTrigger) {
					return new Duo<>(getTarget(modifiedTrigger), getTarget(unmodifiedTrigger));
				}
				
				private TRGT getTarget(SRC src) {
					return cascadeOne.getTargetProvider().get(src);
				}
			});
			if (orphanRemoval) {
				srcPersisterListener.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, cascadeOne.getTargetProvider()));
			}
		}
		
		@Override
		protected void addDeleteCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
									  Persister<TRGT, ID, Table> targetPersister,
									  PersisterListener<SRC, ID> srcPersisterListener, boolean orphanRemoval) {
			if (orphanRemoval) {
				// adding cascade treatment: target is deleted after source deletion (because of foreign key constraint)
				Predicate<TRGT> deletionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(not(targetPersister.getMappingStrategy().getIdMappingStrategy()::isNew));
				srcPersisterListener.addDeleteListener(new AfterDeleteSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
				// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
				srcPersisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
			} // else : no target entities deletion asked (no delete orphan) : nothing more to do than deleting the source entity
		}
	}
	
	
	private static class RelationOwnedByTargetConfigurer<SRC, TRGT, ID> extends ConfigurerTemplate<SRC, TRGT, ID> {
		
		private static final Function NULL_RETURNING_FUNCTION = trgt -> null;
		
		/**
		 * Foreign key column value store, for update and delete cases : store column value per bean,
		 * can be a nullifying function, or an id provider to the referenced source entity.
		 * Implemented as a ThreadLocal because we can hardly cross layers and methods to pass such a value.
		 * Cleaned post update or delete.
		 * All targets are
		 */
		private final ThreadLocal<Map<TRGT, Function<TRGT, SRC>>> foreignKeyValueProvider = ThreadLocal.withInitial(HashMap::new);
		
		// Fixes relation between source and target at load time, stored as an instance field to pass it from creating method to consuming method
		// but shouldn't be kept, bad design but couldn't find another solution
		private BeanRelationFixer<SRC, TRGT> beanRelationFixer = null;
		
		// Gets source from target to update relation at update time, stored as an instance field to pass it from creating method to consuming method
		// but shouldn't be kept, bad design but couldn't find another solution
		private Function<TRGT, SRC> reverseGetter;
		private Column rightColumn;
		
		private RelationOwnedByTargetConfigurer(PersistenceContext persistenceContext) {
			super(persistenceContext);
		}
		
		@Override
		protected <T extends Table<T>> Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																					  ClassMappingStrategy<SRC, ID, T> mappingStrategy,
																					  IReversibleAccessor<SRC, TRGT> targetAccessor,
																					  ClassMappingStrategy<TRGT, ID, Table> targetMappingStrategy,
																					  ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			
			// left column is always left table primary key
			Column leftColumn = Iterables.first(mappingStrategy.getTargetTable().getPrimaryKey().getColumns());
			// right column depends on relation owner
			rightColumn = null;
			if (cascadeOne.getReverseColumn() != null) {
				rightColumn = cascadeOne.getReverseColumn();
			}
			IMutator<SRC, TRGT> sourceIntoTargetFixer = targetAccessor.toMutator();
			if (cascadeOne.getReverseGetter() != null) {
				AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(cascadeOne.getReverseGetter());
				MemberDefinition memberDefinition = MemberDefinition.giveMemberDefinition(localReverseGetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), localReverseGetter, memberDefinition);
				
				// we take advantage of forign key computing and presence of MemberDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
				IMutator<TRGT, SRC> targetIntoSourceFixer = Accessors.mutatorByMethod(memberDefinition.getDeclaringClass(), memberDefinition.getName());
				beanRelationFixer = (target, input) -> {
					// fixing target on source side
					targetIntoSourceFixer.set(input, target);
					// fixing source on target side
					sourceIntoTargetFixer.set(target, input);
				};
				this.reverseGetter = cascadeOne.getReverseGetter();
			} else if (cascadeOne.getReverseSetter() != null) {
				ValueAccessPoint reverseSetter = Accessors.mutatorByMethodReference(cascadeOne.getReverseSetter());
				MemberDefinition memberDefinition = MemberDefinition.giveMemberDefinition(reverseSetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), reverseSetter, memberDefinition);
				
				AccessorByMethod<TRGT, SRC> accessorByMethod = Accessors.accessorByMethod(memberDefinition.getDeclaringClass(), memberDefinition.getName());
				SerializableFunction<TRGT, SRC> localReverseGetter = accessorByMethod::get;
				
				// we take advantage of forign key computing and presence of MemberDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
				beanRelationFixer = (target, input) -> {
					// fixing target on source side
					cascadeOne.getReverseSetter().accept(input, target);
					// fixing source on target side
					sourceIntoTargetFixer.set(target, input);
				};
				this.reverseGetter = localReverseGetter;
			} else {
				// non bidirectional relation (both getter and setter are null)
			}
			
			// adding foreign key constraint
			String foreignKeyName = foreignKeyNamingStrategy.giveName(rightColumn, leftColumn);
			rightColumn.getTable().addForeignKey(foreignKeyName, rightColumn, leftColumn);
			return new Duo<>(leftColumn, rightColumn);
		}
		
		private Column createOrUseReverseColumn(ClassMappingStrategy<TRGT, ID, Table> targetMappingStrategy, Column reverseColumn, ValueAccessPoint reverseGetter, MemberDefinition memberDefinition) {
			if (reverseColumn == null) {
				// no reverse column was given, so we look for the one mapped under the reverse getter
				reverseColumn = targetMappingStrategy.getMainMappingStrategy().getPropertyToColumn().get(reverseGetter);
				if (reverseColumn == null) {
					// no coumn is defined under the getter, then we have to create one
					reverseColumn = targetMappingStrategy.getTargetTable().addColumn(memberDefinition.getName(), memberDefinition.getMemberType());
				}
			}
			return reverseColumn;
		}
		
		@Override
		protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(IReversibleAccessor<SRC, TRGT> targetAccessor) {
			return this.beanRelationFixer;
		}
		
		@Override
		protected void addInsertCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
									  Persister<TRGT, ID, Table> targetPersister,
									  PersisterListener<SRC, ID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addInsertListener(new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider()));
			}
			// adding cascade treatment: after source insert, target is inserted to comply with foreign key constraint
			Predicate<TRGT> insertionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(targetPersister.getMappingStrategy().getIdMappingStrategy()::isNew);
			srcPersisterListener.addInsertListener(new AfterInsertSupport<>(targetPersister::insert, cascadeOne.getTargetProvider()::get, insertionPredicate));
			targetPersister.getMappingStrategy().addSilentColumnInserter(rightColumn, this.reverseGetter);
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
										Persister<TRGT, ID, Table> targetPersister,
										PersisterListener<SRC, ID> srcPersisterListener,
										boolean orphanRemoval) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addUpdateListener(new MandatoryRelationCheckingBeforeUpdateListener<>(cascadeOne.getTargetProvider()));
			}
			// adding cascade treatment
			// - insert non-persisted target instances to fulfill foreign key requirement
			srcPersisterListener.addUpdateListener(new BeforeUpdateSupport<>(
					// we insert new instances
					(it, b) -> targetPersister.insert(Iterables.collectToList(it, Duo::getLeft)),
					cascadeOne.getTargetProvider()::get,
					// we only keep targets of modified instances, non null and not yet persisted
					Predicates.predicate(Duo::getLeft, Predicates.<TRGT>predicate(Objects::nonNull).and(targetPersister.getMappingStrategy()::isNew))
			));
			
			targetPersister.getMappingStrategy().addSilentColumnUpdater(rightColumn, trgt ->
					foreignKeyValueProvider.get().getOrDefault(trgt, reverseGetter).apply(trgt));
			
			// - after source update, target is updated too
			srcPersisterListener.addUpdateListener(new UpdateListener<SRC>() {
				
				@Override
				public void afterUpdate(Iterable<UpdatePayload<? extends SRC, ?>> payloads, boolean allColumnsStatement) {
					ThreadLocals.doWithThreadLocal(foreignKeyValueProvider, HashMap::new, (Runnable) () -> {
						List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
								e -> {
									TRGT targetOfModified = getTarget(e.getEntities().getLeft());
									TRGT targetOfUnmodified = getTarget(e.getEntities().getRight());
									if (targetOfModified == null && targetOfUnmodified != null) {
										// "REMOVED"
										// relation is nullified : relation column should be nullified too
										foreignKeyValueProvider.get().put(targetOfUnmodified, NULL_RETURNING_FUNCTION);
										return false;
									} else if (targetOfModified != null && targetOfUnmodified == null) {
										// "ADDED"
										// relation is set, we fully update modified bean, then its properties will be updated too
										return true;
									} else {
										// "HELD"
										return targetOfModified != null;
										// is an optimisation of :
										// - if targetOfModified != null && targetOfUnmodified != null
										//   then return true, because instance mays differs, and this case will be treated by persister.update
										//   (..)
										// - else : both instance are null => nothing to do, return false
									}
								},
								e -> getTargets(e.getEntities().getLeft(), e.getEntities().getRight()),
								ArrayList::new);
						targetPersister.update(targetsToUpdate, allColumnsStatement);
						targetPersister.updateById(foreignKeyValueProvider.get().keySet());
					});
				}
				
				private Duo<TRGT, TRGT> getTargets(SRC modifiedTrigger, SRC unmodifiedTrigger) {
					return new Duo<>(getTarget(modifiedTrigger), getTarget(unmodifiedTrigger));
				}
				
				private TRGT getTarget(SRC src) {
					return cascadeOne.getTargetProvider().get(src);
				}
			});
			if (orphanRemoval) {
				srcPersisterListener.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, cascadeOne.getTargetProvider()));
			}
		}
		
		@Override
		protected void addDeleteCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
									  Persister<TRGT, ID, Table> targetPersister,
									  PersisterListener<SRC, ID> srcPersisterListener, boolean deleteTargetEntities) {
			if (deleteTargetEntities) {
				// adding cascade treatment: target is deleted before source deletion (because of foreign key constraint)
				Predicate<TRGT> deletionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(not(targetPersister.getMappingStrategy().getIdMappingStrategy()::isNew));
				srcPersisterListener.addDeleteListener(new BeforeDeleteSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
				// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
				srcPersisterListener.addDeleteByIdListener(new BeforeDeleteByIdSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
			} else {
				// no target entities deletion asked (no delete orphan) : we only need to nullify the relation
				srcPersisterListener.addDeleteListener(new NullifyRelationColumnBeforeDelete(cascadeOne, targetPersister));
			}
		}
		
		private class NullifyRelationColumnBeforeDelete implements DeleteListener<SRC> {
			
			private final CascadeOne<SRC, TRGT, ID> cascadeOne;
			private final Persister<TRGT, ID, Table> targetPersister;
			
			private NullifyRelationColumnBeforeDelete(CascadeOne<SRC, TRGT, ID> cascadeOne, Persister<TRGT, ID, Table> targetPersister) {
				this.cascadeOne = cascadeOne;
				this.targetPersister = targetPersister;
			}
			
			@Override
			public void beforeDelete(Iterable<SRC> entities) {
				ThreadLocals.doWithThreadLocal(foreignKeyValueProvider, HashMap::new, (Runnable) () ->
					this.targetPersister.updateById(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).peek(trgt ->
							foreignKeyValueProvider.get().put(trgt, (Function<TRGT, SRC>) NULL_RETURNING_FUNCTION))
							.collect(Collectors.toList())
					)
				);
			}
			
			private TRGT getTarget(SRC src) {
				TRGT target = cascadeOne.getTargetProvider().get(src);
				// We only delete persisted instances (for logic and to prevent from non matching row count error)
				return target != null && !targetPersister.getMappingStrategy().getIdMappingStrategy().isNew(target) ? target : null;
			}
		}
	}
	
	public static class MandatoryRelationCheckingBeforeInsertListener<C> implements InsertListener<C> {
		
		private final IAccessor<C, ?> targetAccessor;
		
		public MandatoryRelationCheckingBeforeInsertListener(IAccessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeInsert(Iterable<? extends C> entities) {
			for (C pawn : entities) {
				Object modifiedTarget = targetAccessor.get(pawn);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(pawn, targetAccessor);
				}
			}
		}
	}
	
	public static class MandatoryRelationCheckingBeforeUpdateListener<C> implements UpdateListener<C> {
		
		private final IAccessor<C, ?> targetAccessor;
		
		public MandatoryRelationCheckingBeforeUpdateListener(IAccessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeUpdate(Iterable<UpdatePayload<? extends C, ?>> payloads, boolean allColumnsStatement) {
			for (UpdatePayload<? extends C, ?> payload : payloads) {
				C modifiedEntity = payload.getEntities().getLeft();
				Object modifiedTarget = targetAccessor.get(modifiedEntity);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(modifiedEntity, targetAccessor);
				}
			}
		}
	}
	
	public static RuntimeMappingException newRuntimeMappingException(Object pawn, ValueAccessPoint accessor) {
		return new RuntimeMappingException("Non null value expected for relation "
				+ MemberDefinition.toString(accessor) + " on object " + pawn);
	}
	
	private static class OrphanRemovalOnUpdate<SRC, TRGT> implements UpdateListener<SRC> {
		
		private final Persister<TRGT, ?, ?> targetPersister;
		private final IAccessor<SRC, TRGT> targetAccessor;
		
		private OrphanRemovalOnUpdate(Persister<TRGT, ?, ?> targetPersister, IAccessor<SRC, TRGT> targetAccessor) {
			this.targetPersister = targetPersister;
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void afterUpdate(Iterable<UpdatePayload<? extends SRC, ?>> payloads, boolean allColumnsStatement) {
				List<TRGT> targetsToDeleteUpdate = Iterables.collect(payloads,
						// targets of nullified relations don't need to be updated 
						e -> getTarget(e.getEntities().getLeft()) == null,
						e -> getTarget(e.getEntities().getRight()),
						ArrayList::new);
				targetPersister.delete(targetsToDeleteUpdate);
		}
		
		private TRGT getTarget(SRC src) {
			return targetAccessor.get(src);
		}
	}
}
