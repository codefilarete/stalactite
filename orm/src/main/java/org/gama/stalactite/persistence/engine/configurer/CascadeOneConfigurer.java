package org.gama.stalactite.persistence.engine.configurer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeUpdateSupport;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeRowTransformer;
import org.gama.stalactite.persistence.engine.runtime.IJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.ShadowColumnValueProvider;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinder;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.function.Predicates.not;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.READ_ONLY;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <ID> identifier type of target entities
 * @author Guillaume Mary
 */
public class CascadeOneConfigurer<SRC, TRGT, ID> {
	
	private final Dialect dialect;
	private final IConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	private final PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder;
	
	public CascadeOneConfigurer(Dialect dialect,
								IConnectionConfiguration connectionConfiguration,
								PersisterRegistry persisterRegistry,
								PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		this.targetPersisterBuilder = targetPersisterBuilder;
	}
	
	public <T extends Table<T>> void appendCascade(
			CascadeOne<SRC, TRGT, ID> cascadeOne,
			IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy joinColumnNamingStrategy) {
		
		ConfigurerTemplate<SRC, TRGT, ID> configurer;
		if (cascadeOne.isRelationOwnedByTarget()) {
			configurer = new RelationOwnedByTargetConfigurer<>(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		} else {
			configurer = new RelationOwnedBySourceConfigurer<>(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
		configurer.appendCascade(cascadeOne, sourcePersister, foreignKeyNamingStrategy, joinColumnNamingStrategy);
	}
	
	private abstract static class ConfigurerTemplate<SRC, TRGT, ID> {
		
		protected final Dialect dialect;
		protected final IConnectionConfiguration connectionConfiguration;
		private final PersisterRegistry persisterRegistry;
		private final PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder;
		
		protected ConfigurerTemplate(Dialect dialect,
									 IConnectionConfiguration connectionConfiguration,
									 PersisterRegistry persisterRegistry,
									 PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder) {
			this.dialect = dialect;
			this.connectionConfiguration = connectionConfiguration;
			this.persisterRegistry = persisterRegistry;
			this.targetPersisterBuilder = targetPersisterBuilder;
		}
		
		protected IConfiguredPersister<TRGT, ID> appendCascade(
				CascadeOne<SRC, TRGT, ID> cascadeOne,
				IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
				ForeignKeyNamingStrategy foreignKeyNamingStrategy,
				ColumnNamingStrategy joinColumnNamingStrategy) {
			
			RelationMode maintenanceMode = cascadeOne.getRelationMode();
			if (maintenanceMode == ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(ASSOCIATION_ONLY + " is only relevent for one-to-many association");
			}
			IEntityMappingStrategy<SRC, ID, ?> mappingStrategy = sourcePersister.getMappingStrategy();
			if (mappingStrategy.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
				throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
			}
			
			IReversibleAccessor<SRC, TRGT> targetAccessor = cascadeOne.getTargetProvider();
			
			IEntityConfiguredPersister<TRGT, ID> targetPersister = targetPersisterBuilder
					// please note that even if no table is found in configuration, build(..) will create one
					.build(dialect, connectionConfiguration, persisterRegistry, 
							nullable(cascadeOne.getTargetTable()).getOr(nullable(cascadeOne.getReverseColumn()).map(Column::getTable).get()));
			IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy = targetPersister.getMappingStrategy();
			
			// Finding joined columns
			Duo<Column, Column> foreignKeyColumns = determineForeignKeyColumns(cascadeOne, mappingStrategy, targetAccessor,
					targetMappingStrategy, foreignKeyNamingStrategy, joinColumnNamingStrategy);
			
			Column leftColumn = foreignKeyColumns.getLeft();
			Column rightColumn = foreignKeyColumns.getRight();
			
			// selection is always present (else configuration is nonsense !)
			BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer(targetAccessor);
			addSelectCascade(cascadeOne, sourcePersister, (IEntityConfiguredJoinedTablesPersister<TRGT, ID>) targetPersister, leftColumn, rightColumn, beanRelationFixer);
			
			// additionnal cascade
			boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
			boolean writeAuthorized = maintenanceMode != READ_ONLY;
			if (writeAuthorized) {
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addInsertCascade(cascadeOne, targetPersister, sourcePersister);
				addUpdateCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
				addDeleteCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
			}
			
			return targetPersister;
		}
		
		protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(IReversibleAccessor<SRC, TRGT> targetAccessor) {
			IMutator<SRC, TRGT> targetSetter = targetAccessor.toMutator();
			return BeanRelationFixer.of(targetSetter::set);
		}
		
		protected abstract Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																							   IEntityMappingStrategy<SRC, ID, ?> mappingStrategy,
																							   IReversibleAccessor<SRC, TRGT> targetAccessor,
																							   IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy,
																							   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																							   ColumnNamingStrategy joinColumnNamingStrategy);
		
		@SuppressWarnings("squid:S1172")	// argument targetPersister is used by subclasses
		protected void addInsertCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addInsertListener(new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider()));
			}
		}
		
		protected abstract void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne, IEntityConfiguredPersister<TRGT, ID> targetPersister,
												 IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener, boolean orphanRemoval);
		
		protected abstract void addDeleteCascade(CascadeOne<SRC, TRGT, ID> cascadeOne, IEntityConfiguredPersister<TRGT, ID> targetPersister,
												 IPersisterListener<SRC, ID> srcPersisterListener, boolean orphanRemoval);
		
		protected <T1 extends Table<T1>, T2 extends Table<T2>, P extends IJoinedTablesPersister<SRC, ID> & IPersisterListener<SRC, ID>> void addSelectCascade(
				CascadeOne<SRC, TRGT, ID> cascadeOne,
				P sourcePersister,
				IConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
				Column<T1, ID> leftColumn,
				Column<T2, ID> rightColumn,
				BeanRelationFixer<SRC, TRGT> beanRelationFixer) {
			
			// we add target subgraph joins to the one that was created
			targetPersister.joinAsOne(sourcePersister, leftColumn, rightColumn, beanRelationFixer, cascadeOne.isNullable());
			
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
					List collect = Iterables.collectToList(result, cascadeOne.getTargetProvider()::get);
					// NB: entity can be null when loading relation, we skip nulls to prevent a NPE
					collect.removeIf(java.util.Objects::isNull);
					targetSelectListener.afterSelect(collect);
				}

				@Override
				public void onError(Iterable<ID> ids, RuntimeException exception) {
					// since ids are not those of its entities, we should not pass them as argument
					targetSelectListener.onError(Collections.emptyList(), exception);
				}
			});
		}
		
	}
	
	private static class RelationOwnedBySourceConfigurer<SRC, TRGT, ID> extends ConfigurerTemplate<SRC, TRGT, ID> {
		
		private RelationOwnedBySourceConfigurer(Dialect dialect,
												IConnectionConfiguration connectionConfiguration,
												PersisterRegistry persisterRegistry,
												PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder) {
			super(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
		
		@Override
		protected IConfiguredPersister<TRGT, ID> appendCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
															   IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
															   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
															   ColumnNamingStrategy joinColumnNamingStrategy) {
			IConfiguredPersister<TRGT, ID> targetPersister = super.appendCascade(cascadeOne, sourcePersister, foreignKeyNamingStrategy, joinColumnNamingStrategy);
			Column owningColumn = sourcePersister.getMappingStrategy().getPropertyToColumn().get(cascadeOne.getTargetProvider());
			// we have to register a parameter binder for target entity so inserts & updates can get value from the property, without it, engine
			// tries to find a binder for target entity which doesn't exist 
			registerEntityBinder(owningColumn, targetPersister.getMappingStrategy(), dialect);
			
			return targetPersister;
		}
				
		@Override
		protected Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																					  IEntityMappingStrategy<SRC, ID, ?> srcMappingStrategy,
																					  IReversibleAccessor<SRC, TRGT> targetAccessor,
																					  IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy,
																					  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																					  ColumnNamingStrategy joinColumnNamingStrategy) {
			// targetAccessor may not be the one that was declared in propertyToColumn, so we need to wrap them into a more flexible search structure : ValueAccessPointMap
			Column<Table, ID> leftColumn = (Column<Table, ID>) new ValueAccessPointMap<Column>(srcMappingStrategy.getPropertyToColumn()).get(targetAccessor);
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
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
			srcPersisterListener.addInsertListener(new BeforeInsertSupport<>(targetPersister::persist, cascadeOne.getTargetProvider()::get, Objects::nonNull));
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener,
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
				public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
					List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
							// targets of nullified relations don't need to be updated 
							e -> getTarget(e.getLeft()) != null,
							e -> getTargets(e.getLeft(), e.getRight()),
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
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IPersisterListener<SRC, ID> srcPersisterListener, boolean orphanRemoval) {
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
		 * Foreign key column value store, for update and delete cases : stores column value per bean,
		 * can be a nullifying function, or an id provider to the referenced source entity.
		 * Implemented as a ThreadLocal because we can hardly cross layers and methods to pass such a value.
		 * Cleaned after update and delete.
		 */
		private final ThreadLocal<Map<TRGT, Function<TRGT, SRC>>> currentForeignKeyValueProvider = ThreadLocal.withInitial(HashMap::new);
		
		// Fixes relation between source and target at load time, stored as an instance field to pass it from creating method to consuming method
		// but shouldn't be kept, bad design but couldn't find another solution
		private BeanRelationFixer<SRC, TRGT> beanRelationFixer = null;
		
		// Gets source from target to update relation at update time, stored as an instance field to pass it from creating method to consuming method
		// but shouldn't be kept, bad design but couldn't find another solution
		private Function<TRGT, SRC> reverseGetter;
		@SuppressWarnings("squid:S2259")
		private Column<Table, SRC> rightColumn;
		
		private RelationOwnedByTargetConfigurer(Dialect dialect,
												IConnectionConfiguration connectionConfiguration,
												PersisterRegistry persisterRegistry,
												PersisterBuilderImpl<TRGT, ID> targetPersisterBuilder) {
			super(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
		
		@Override
		protected Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, ID> cascadeOne,
																					  IEntityMappingStrategy<SRC, ID, ?> mappingStrategy,
																					  IReversibleAccessor<SRC, TRGT> targetAccessor,
																					  IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy,
																					  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																					  ColumnNamingStrategy joinColumnNamingStrategy) {
			
			// left column is always left table primary key
			Column<Table, ID> leftColumn = (Column<Table, ID>) Iterables.first(mappingStrategy.getTargetTable().getPrimaryKey().getColumns());
			// right column depends on relation owner
			if (cascadeOne.getReverseColumn() != null) {
				rightColumn = cascadeOne.getReverseColumn();
			}
			IMutator<SRC, TRGT> sourceIntoTargetFixer = targetAccessor.toMutator();
			if (cascadeOne.getReverseGetter() != null) {
				AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(cascadeOne.getReverseGetter());
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(localReverseGetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), localReverseGetter, accessorDefinition, joinColumnNamingStrategy);
				registerEntityBinder(rightColumn, mappingStrategy, dialect);
				
				// we take advantage of foreign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
				IMutator<TRGT, SRC> targetIntoSourceFixer = Accessors.mutatorByMethod(accessorDefinition.getDeclaringClass(), accessorDefinition.getName());
				beanRelationFixer = (src, target) -> {
					// fixing source on target
					if (target != null) {	// prevent NullPointerException, actually means no linked entity (null relation), so nothing to do
						targetIntoSourceFixer.set(target, src);
					}
					// fixing target on source
					sourceIntoTargetFixer.set(src, target);
				};
				this.reverseGetter = cascadeOne.getReverseGetter();
			} else if (cascadeOne.getReverseSetter() != null) {
				ValueAccessPoint reverseSetter = Accessors.mutatorByMethodReference(cascadeOne.getReverseSetter());
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(reverseSetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), reverseSetter, accessorDefinition,
						joinColumnNamingStrategy);
				registerEntityBinder(rightColumn, mappingStrategy, dialect);
				
				IAccessor<TRGT, SRC> accessor = Accessors.accessor(accessorDefinition.getDeclaringClass(), accessorDefinition.getName());
				// we take advantage of forign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
				beanRelationFixer = (target, input) -> {
					// fixing target on source side
					cascadeOne.getReverseSetter().accept(input, target);
					// fixing source on target side
					sourceIntoTargetFixer.set(target, input);
				};
				this.reverseGetter = accessor::get;
			} // else : non bidirectional relation (both getter and setter are null), nothing to do
			
			// adding foreign key constraint
			String foreignKeyName = foreignKeyNamingStrategy.giveName(rightColumn, leftColumn);
			// Note that rightColumn can't be null because RelationOwnedByTargetConfigurer is used when one of cascadeOne.getReverseColumn(),
			// cascadeOne.getReverseGetter() and cascadeOne.getReverseSetter() is not null
			rightColumn.getTable().addForeignKey(foreignKeyName, rightColumn, leftColumn);
			return new Duo<>(leftColumn, rightColumn);
		}
		
		private Column createOrUseReverseColumn(IEntityMappingStrategy<TRGT, ID, ?> targetMappingStrategy, Column reverseColumn,
												ValueAccessPoint reverseGetter, AccessorDefinition accessorDefinition,
												ColumnNamingStrategy joinColumnNamingStrategy) {
			if (reverseColumn == null) {
				// no reverse column was given, so we look for the one mapped under the reverse getter
				reverseColumn = targetMappingStrategy.getPropertyToColumn().get(reverseGetter);
				if (reverseColumn == null) {
					// no column is defined under the getter, then we have to create one
					reverseColumn = targetMappingStrategy.getTargetTable().addColumn(joinColumnNamingStrategy.giveName(accessorDefinition), accessorDefinition.getMemberType());
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
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// adding cascade treatment: after source insert, target is inserted to comply with foreign key constraint
			srcPersisterListener.addInsertListener(new AfterInsertSupport<>(targetPersister::persist, cascadeOne.getTargetProvider()::get, Objects::nonNull));
			targetPersister.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<>(rightColumn, this.reverseGetter));
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, ID> cascadeOne,
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, ID> srcPersisterListener,
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
			
			targetPersister.getMappingStrategy().addShadowColumnUpdate(new ShadowColumnValueProvider<>(rightColumn, (TRGT trgt) ->
					currentForeignKeyValueProvider.get().getOrDefault(trgt, reverseGetter).apply(trgt)));
			
			// - after source update, target is updated too
			srcPersisterListener.addUpdateListener(new UpdateListener<SRC>() {
				
				@Override
				public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
					ThreadLocals.doWithThreadLocal(currentForeignKeyValueProvider, HashMap::new, (Consumer<Map<TRGT, Function<TRGT,SRC>>>) fkValueProvider -> {
						List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
								e -> {
									TRGT targetOfModified = getTarget(e.getLeft());
									TRGT targetOfUnmodified = getTarget(e.getRight());
									if (targetOfModified == null && targetOfUnmodified != null) {
										// "REMOVED"
										// relation is nullified : relation column should be nullified too
										fkValueProvider.put(targetOfUnmodified, NULL_RETURNING_FUNCTION);
										return false;
									} else if (targetOfModified != null && targetOfUnmodified == null) {
										// "ADDED"
										// relation is set, we fully update modified bean, then its properties will be updated too
										return true;
									} else {
										// "HELD"
										
										// Nullifying reverse column of detached target : this is made in case of dev who didn't nullify reverse property
										// of old target when new target was set, by doing so we avoid old target to still point to source entity
										// in databse through the foreign key, which throw constraint exception when source is deleted 
										if (reverseGetter.apply(targetOfUnmodified) != null	// reverse property isn't nullify
											&& !targetPersister.getMappingStrategy().isNew(targetOfUnmodified)	// this is for safety, not sure of any use case
											// Was target entity reassigned to another source ? if true we should not nullify column because it will
											// be maintained by targets update, nullifying it would add an extra-useless SQL order
											&& !targetPersister.getMappingStrategy().getId(targetOfUnmodified).equals(targetPersister.getMappingStrategy().getId(targetOfModified))
										) {
											fkValueProvider.put(targetOfUnmodified, NULL_RETURNING_FUNCTION);
										}
										return targetOfModified != null;
										// is an optimisation of :
										// - if targetOfModified != null && targetOfUnmodified != null
										//   then return true, because instance mays differs, and this case will be treated by persister.update
										//   (..)
										// - else : both instance are null => nothing to do, return false
									}
								},
								e -> getTargets(e.getLeft(), e.getRight()),
								ArrayList::new);
						targetPersister.update(targetsToUpdate, allColumnsStatement);
						targetPersister.updateById(currentForeignKeyValueProvider.get().keySet());
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
										IEntityConfiguredPersister<TRGT, ID> targetPersister,
										IPersisterListener<SRC, ID> srcPersisterListener, boolean deleteTargetEntities) {
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
			private final IEntityConfiguredPersister<TRGT, ID> targetPersister;
			
			private NullifyRelationColumnBeforeDelete(CascadeOne<SRC, TRGT, ID> cascadeOne, IEntityConfiguredPersister<TRGT, ID> targetPersister) {
				this.cascadeOne = cascadeOne;
				this.targetPersister = targetPersister;
			}
			
			@Override
			public void beforeDelete(Iterable<SRC> entities) {
				ThreadLocals.doWithThreadLocal(currentForeignKeyValueProvider, HashMap::new, (Consumer<Map<TRGT, Function<TRGT,SRC>>>) fkValueProvider ->
					this.targetPersister.updateById(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).peek(trgt ->
							fkValueProvider.put(trgt, (Function<TRGT, SRC>) NULL_RETURNING_FUNCTION))
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
		public void beforeUpdate(Iterable<? extends Duo<? extends C, ? extends C>> payloads, boolean allColumnsStatement) {
			for (Duo<? extends C, ? extends C> payload : payloads) {
				C modifiedEntity = payload.getLeft();
				Object modifiedTarget = targetAccessor.get(modifiedEntity);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(modifiedEntity, targetAccessor);
				}
			}
		}
	}
	
	public static RuntimeMappingException newRuntimeMappingException(Object pawn, ValueAccessPoint accessor) {
		return new RuntimeMappingException("Non null value expected for relation "
				+ AccessorDefinition.toString(accessor) + " on object " + pawn);
	}
	
	private static class OrphanRemovalOnUpdate<SRC, TRGT> implements UpdateListener<SRC> {
		
		private final IEntityConfiguredPersister<TRGT, ?> targetPersister;
		private final IAccessor<SRC, TRGT> targetAccessor;
		private final IAccessor<TRGT, ?> targetIdAccessor;
		
		private OrphanRemovalOnUpdate(IEntityConfiguredPersister<TRGT, ?> targetPersister, IAccessor<SRC, TRGT> targetAccessor) {
			this.targetPersister = targetPersister;
			this.targetAccessor = targetAccessor;
			this.targetIdAccessor = targetPersister.getMappingStrategy().getIdMappingStrategy().getIdAccessor()::getId;
		}
		
		@Override
		public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
				List<TRGT> targetsToDeleteUpdate = new ArrayList<>();
				payloads.forEach(duo -> {
					TRGT newTarget = getTarget(duo.getLeft());
					TRGT oldTarget = getTarget(duo.getRight());
					// nullified relations and changed ones must be removed (orphan removal)
					// TODO : one day we'll have to cover case of reused instance in same graph : one of the relation must handle it, not both,
					//  "else a marked instance" system must be implemented
					if (newTarget == null || (oldTarget != null && !targetIdAccessor.get(newTarget).equals(targetIdAccessor.get(oldTarget)))) {
						targetsToDeleteUpdate.add(oldTarget);
					}
				});
				targetPersister.delete(targetsToDeleteUpdate);
		}
		
		private TRGT getTarget(SRC src) {
			return targetAccessor.get(src);
		}
	}
	
	/**
	 * Registers given {@link Column} into {@link Dialect} binder registry. Made for one-to-one cases because relation-owning property must be persisted
	 * as a foreign key, so identifier must be extracted from target entity.
	 * 
	 * @param owningColumn the column that owns relation property
	 * @param mappingStrategy mapping strategy of entity targeted by column (may be root or child dependeing of ownership) 
	 * @param dialect dialect to register binders
	 * @param <TRGT> target type of the column
	 * @param <ID> identifier type
	 */
	private static <TRGT, ID> void registerEntityBinder(Column owningColumn, IEntityMappingStrategy<TRGT, ID, ?> mappingStrategy, Dialect dialect) {
		// Note : for now only single primary key column is supported
		IdMappingStrategy<TRGT, ID> targetIdMappingStrategy = mappingStrategy.getIdMappingStrategy();
		Column targetPrimaryKey = ((SimpleIdentifierAssembler) targetIdMappingStrategy.getIdentifierAssembler()).getColumn();
		IReversibleAccessor targetIdAccessor = ((SinglePropertyIdAccessor) targetIdMappingStrategy.getIdAccessor()).getIdAccessor();
		AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(targetIdAccessor);
		
		// Binding sql column type
		String targetPrimaryKeySqlTypeName = dialect.getJavaTypeToSqlTypeMapping().getTypeName(targetPrimaryKey);
		dialect.getJavaTypeToSqlTypeMapping().put(owningColumn, targetPrimaryKeySqlTypeName);
		// Binding entity type : binder will get entity identifier
		ParameterBinder ownerBinder = dialect.getColumnBinderRegistry().getBinder(accessorDefinition.getMemberType());
		dialect.getColumnBinderRegistry().register(owningColumn, new EntityBinder<>(ownerBinder, targetIdAccessor));
	}
	
	private static class EntityBinder<E> extends NullAwareParameterBinder<E> {
		
		private final ParameterBinder ownerBinder;
		private final IReversibleAccessor targetIdAccessor;
		
		private EntityBinder(ParameterBinder ownerBinder, IReversibleAccessor targetIdAccessor) {
			super(new ParameterBinder<E>() {
				@Override
				public void set(PreparedStatement preparedStatement, int valueIndex, E value) throws SQLException {
					ownerBinder.set(preparedStatement, valueIndex, targetIdAccessor.get(value));
				}
				
				/**
				 * This is never used because it should return an entity which can't be build here.
				 * It will be by {@link EntityMappingStrategyTreeRowTransformer}
				 * Hence this implementation returns null
				 */
				@Override
				public E doGet(ResultSet resultSet, String columnName) {
					return null;
				}
			});
			this.ownerBinder = ownerBinder;
			this.targetIdAccessor = targetIdAccessor;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof EntityBinder)) return false;
			
			EntityBinder<?> that = (EntityBinder<?>) o;
			
			if (!ownerBinder.equals(that.ownerBinder)) return false;
			return targetIdAccessor.equals(that.targetIdAccessor);
		}
	}
}
