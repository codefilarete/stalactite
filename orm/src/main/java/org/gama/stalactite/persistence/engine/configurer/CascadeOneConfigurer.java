package org.gama.stalactite.persistence.engine.configurer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.Accessors;
import org.gama.reflection.IAccessor;
import org.gama.reflection.IMutator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeUpdateSupport;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.IJoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.ShadowColumnValueProvider;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.WriteOperation;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.function.Predicates.not;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.READ_ONLY;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of target entities
 * @author Guillaume Mary
 */
public class CascadeOneConfigurer<SRC, TRGT, SRCID, TRGTID> {
	
	private final Dialect dialect;
	private final IConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	private final PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder;
	
	public CascadeOneConfigurer(Dialect dialect,
								IConnectionConfiguration connectionConfiguration,
								PersisterRegistry persisterRegistry,
								PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		this.targetPersisterBuilder = targetPersisterBuilder;
	}
	
	public <T extends Table<T>> void appendCascade(
			CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
			IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy joinColumnNamingStrategy) {
		
		ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID> configurer;
		if (cascadeOne.isRelationOwnedByTarget()) {
			configurer = new RelationOwnedByTargetConfigurer<>(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		} else {
			configurer = new RelationOwnedBySourceConfigurer<>(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
		configurer.appendCascade(cascadeOne, sourcePersister, foreignKeyNamingStrategy, joinColumnNamingStrategy);
	}
	
	private abstract static class ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID> {
		protected IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
		protected final Dialect dialect;
		protected final IConnectionConfiguration connectionConfiguration;
		private final PersisterRegistry persisterRegistry;
		private final PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder;
		
		protected ConfigurerTemplate(Dialect dialect,
									 IConnectionConfiguration connectionConfiguration,
									 PersisterRegistry persisterRegistry,
									 PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
			this.dialect = dialect;
			this.connectionConfiguration = connectionConfiguration;
			this.persisterRegistry = persisterRegistry;
			this.targetPersisterBuilder = targetPersisterBuilder;
		}
		
		protected IConfiguredPersister<TRGT, TRGTID> appendCascade(
				CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
				IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
				ForeignKeyNamingStrategy foreignKeyNamingStrategy,
				ColumnNamingStrategy joinColumnNamingStrategy) {
			
			RelationMode maintenanceMode = cascadeOne.getRelationMode();
			if (maintenanceMode == ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(ASSOCIATION_ONLY + " is only relevent for one-to-many association");
			}
			IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy = sourcePersister.getMappingStrategy();
			if (mappingStrategy.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
				throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
			}
			this.sourcePersister = sourcePersister;
			IReversibleAccessor<SRC, TRGT> targetAccessor = cascadeOne.getTargetProvider();
			
			IEntityConfiguredPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
					// please note that even if no table is found in configuration, build(..) will create one
					.build(dialect, connectionConfiguration, persisterRegistry, 
							nullable(cascadeOne.getTargetTable()).getOr(nullable(cascadeOne.getReverseColumn()).map(Column::getTable).get()));
			IEntityMappingStrategy<TRGT, TRGTID, ?> targetMappingStrategy = targetPersister.getMappingStrategy();
			
			// Finding joined columns
			Duo<Column, Column> foreignKeyColumns = determineForeignKeyColumns(cascadeOne, mappingStrategy,
					targetMappingStrategy, foreignKeyNamingStrategy, joinColumnNamingStrategy);
			
			Column leftColumn = foreignKeyColumns.getLeft();
			Column rightColumn = foreignKeyColumns.getRight();
			
			// selection is always present (else configuration is nonsense !)
			BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer(targetAccessor);
			addSelectCascade(cascadeOne, sourcePersister, (IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID>) targetPersister, leftColumn, rightColumn, beanRelationFixer);
			addWriteCascades(cascadeOne, sourcePersister, targetPersister, maintenanceMode);
			
			return targetPersister;
		}
		
		protected void addWriteCascades(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										RelationMode maintenanceMode) {
			boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
			boolean writeAuthorized = maintenanceMode != READ_ONLY;
			if (writeAuthorized) {
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addInsertCascade(cascadeOne, targetPersister, sourcePersister);
				addUpdateCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
				addDeleteCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
			}
		}
		
		protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(IReversibleAccessor<SRC, TRGT> targetAccessor) {
			IMutator<SRC, TRGT> targetSetter = targetAccessor.toMutator();
			return BeanRelationFixer.of(targetSetter::set);
		}
		
		protected abstract Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
																		  IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
																		  IEntityMappingStrategy<TRGT, TRGTID, ?> targetMappingStrategy,
																		  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																		  ColumnNamingStrategy joinColumnNamingStrategy);
		
		@SuppressWarnings("squid:S1172")	// argument targetPersister is used by subclasses
		protected void addInsertCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addInsertListener(new MandatoryRelationCheckingBeforeInsertListener<>(cascadeOne.getTargetProvider()));
			}
		}
		
		protected abstract void addUpdateCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne, IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
												 IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener, boolean orphanRemoval);
		
		protected abstract void addDeleteCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne, IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
												 IPersisterListener<SRC, SRCID> srcPersisterListener, boolean orphanRemoval);
		
		protected <
				T1 extends Table<T1>,
				T2 extends Table<T2>,
				P extends IJoinedTablesPersister<SRC, SRCID> & IPersisterListener<SRC, SRCID>,
				J>
		void addSelectCascade(
				CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
				P sourcePersister,
				IConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
				Column<T1, J> leftColumn,
				Column<T2, J> rightColumn,
				BeanRelationFixer<SRC, TRGT> beanRelationFixer) {
			
			// we add target subgraph joins to the one that was created
			targetPersister.joinAsOne(sourcePersister, leftColumn, rightColumn, beanRelationFixer, cascadeOne.isNullable());
			
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
					List collect = Iterables.collectToList(result, cascadeOne.getTargetProvider()::get);
					// NB: entity can be null when loading relation, we skip nulls to prevent a NPE
					collect.removeIf(java.util.Objects::isNull);
					targetSelectListener.afterSelect(collect);
				}

				@Override
				public void onError(Iterable<SRCID> ids, RuntimeException exception) {
					// since ids are not those of its entities, we should not pass them as argument
					targetSelectListener.onError(Collections.emptyList(), exception);
				}
			});
		}
		
	}
	
	private static class RelationOwnedBySourceConfigurer<SRC, TRGT, SRCID, TRGTID> extends ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID> {
		
		private Column<Table, TRGTID> leftColumn;
		
		private RelationOwnedBySourceConfigurer(Dialect dialect,
												IConnectionConfiguration connectionConfiguration,
												PersisterRegistry persisterRegistry,
												PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
			super(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
				
		@Override
		protected Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
																 IEntityMappingStrategy<SRC, SRCID, ?> srcMappingStrategy,
																 IEntityMappingStrategy<TRGT, TRGTID, ?> targetMappingStrategy,
																 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																 ColumnNamingStrategy joinColumnNamingStrategy) {
			Column<?, SRCID> rightColumn = (Column<?, SRCID>) Iterables.first((Set<Column<?, Object>>) targetMappingStrategy.getTargetTable().getPrimaryKey().getColumns());
			leftColumn = sourcePersister.getMappingStrategy().getTargetTable().addColumn(
					joinColumnNamingStrategy.giveName(AccessorDefinition.giveDefinition(cascadeOne.getTargetProvider())),
					rightColumn.getJavaType()
			);
			
			// According to the nullable option, we specify the ddl schema option
			leftColumn.nullable(cascadeOne.isNullable());
			
			// adding foreign key constraint
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftColumn, rightColumn);
			leftColumn.getTable().addForeignKey(foreignKeyName, leftColumn, rightColumn);
			return new Duo<>(leftColumn, rightColumn);
		}
		
		@Override
		protected void addWriteCascades(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										RelationMode maintenanceMode) {
			// whatever kind of relation maintenance mode asked, we have to insert and update source-to-target link, because we are in relation-owned-by-source
			Function<SRC, TRGTID> targetIdProvider = src -> {
				TRGT trgt = cascadeOne.getTargetProvider().get(src);
				return trgt == null ? null : targetPersister.getMappingStrategy().getId(trgt);
			};
			sourcePersister.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<>(leftColumn, targetIdProvider));
			sourcePersister.getMappingStrategy().addShadowColumnUpdate(new ShadowColumnValueProvider<>(leftColumn, targetIdProvider));
			
			super.addWriteCascades(cascadeOne, sourcePersister, targetPersister, maintenanceMode);
		}
		
		@Override
		protected void addInsertCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
			srcPersisterListener.addInsertListener(new BeforeInsertSupport<>(targetPersister::persist, cascadeOne.getTargetProvider()::get, Objects::nonNull));
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener,
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
		protected void addDeleteCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IPersisterListener<SRC, SRCID> srcPersisterListener, boolean orphanRemoval) {
			if (orphanRemoval) {
				// adding cascade treatment: target is deleted after source deletion (because of foreign key constraint)
				Predicate<TRGT> deletionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(not(targetPersister.getMappingStrategy().getIdMappingStrategy()::isNew));
				srcPersisterListener.addDeleteListener(new AfterDeleteSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
				// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
				srcPersisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(targetPersister::delete, cascadeOne.getTargetProvider()::get, deletionPredicate));
			} // else : no target entities deletion asked (no delete orphan) : nothing more to do than deleting the source entity
		}
	}
	
	
	private static class RelationOwnedByTargetConfigurer<SRC, TRGT, SRCID, TRGTID> extends ConfigurerTemplate<SRC, TRGT, SRCID, TRGTID> {
		
		/**
		 * Foreign key column value store, for update and delete cases : stores column value per bean,
		 * can be a nullifying function, or an id provider to the referenced source entity.
		 * Implemented as a ThreadLocal because we can hardly cross layers and methods to pass such a value.
		 * Cleaned after update and delete.
		 */
		private final ThreadLocal<ForeignKeyValueProvider> currentForeignKeyValueProvider = new ThreadLocal<>();
		
		private class ForeignKeyValueProvider {
			private final Set<ForeignKeyHolder> store = new HashSet<>();
			
			private void add(TRGT target, SRC source) {
				store.add(new ForeignKeyHolder(target, source));
			}
			
			private SRCID giveSourceId(TRGT trgt) {
				return nullable(Iterables.find(store, fk -> fk.modifiedTarget == trgt)).map(x -> x.modifiedSource).map(sourcePersister.getMappingStrategy()::getId).get();
			}
			
			/**
			 * Small class to store relating source and target entities 
			 */
			private class ForeignKeyHolder {
				private final TRGT modifiedTarget;
				private final SRC modifiedSource;
				
				public ForeignKeyHolder(TRGT modifiedTarget, SRC modifiedSource) {
					this.modifiedTarget = modifiedTarget;
					this.modifiedSource = modifiedSource;
				}
			}
		}
		
		// Fixes relation between source and target at load time, stored as an instance field to pass it from creating method to consuming method
		// but shouldn't be kept, bad design but couldn't find another solution
		private BeanRelationFixer<SRC, TRGT> beanRelationFixer = null;
		
		@SuppressWarnings("squid:S2259")
		private Column<Table, SRCID> rightColumn;
		
		private RelationOwnedByTargetConfigurer(Dialect dialect,
												IConnectionConfiguration connectionConfiguration,
												PersisterRegistry persisterRegistry,
												PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
			super(dialect, connectionConfiguration, persisterRegistry, targetPersisterBuilder);
		}
		
		@Override
		protected Duo<Column, Column> determineForeignKeyColumns(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
																 IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
																 IEntityMappingStrategy<TRGT, TRGTID, ?> targetMappingStrategy,
																 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
																 ColumnNamingStrategy joinColumnNamingStrategy) {
			
			// left column is always left table primary key
			Column<Table, SRCID> leftColumn = (Column<Table, SRCID>) Iterables.first(mappingStrategy.getTargetTable().getPrimaryKey().getColumns());
			// right column depends on relation owner
			if (cascadeOne.getReverseColumn() != null) {
				rightColumn = cascadeOne.getReverseColumn();
			}
			IMutator<SRC, TRGT> sourceIntoTargetFixer = cascadeOne.getTargetProvider().toMutator();
			if (cascadeOne.getReverseGetter() != null) {
				AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(cascadeOne.getReverseGetter());
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(localReverseGetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), localReverseGetter, accessorDefinition,
						joinColumnNamingStrategy);
				
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
			} else if (cascadeOne.getReverseSetter() != null) {
				ValueAccessPoint reverseSetter = Accessors.mutatorByMethodReference(cascadeOne.getReverseSetter());
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(reverseSetter);
				// we add a column for reverse mapping if one is not already declared
				rightColumn = createOrUseReverseColumn(targetMappingStrategy, cascadeOne.getReverseColumn(), reverseSetter, accessorDefinition,
						joinColumnNamingStrategy);
				
				// we take advantage of forign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
				beanRelationFixer = (target, input) -> {
					// fixing target on source side
					cascadeOne.getReverseSetter().accept(input, target);
					// fixing source on target side
					sourceIntoTargetFixer.set(target, input);
				};
			}
			else {
				// non bidirectional relation : relation is owned by target without defining any way to fix it in memory
				// we can only fix target on source side
				beanRelationFixer = sourceIntoTargetFixer::set;
			}
			
			// adding foreign key constraint
			String foreignKeyName = foreignKeyNamingStrategy.giveName(rightColumn, leftColumn);
			// Note that rightColumn can't be null because RelationOwnedByTargetConfigurer is used when one of cascadeOne.getReverseColumn(),
			// cascadeOne.getReverseGetter() and cascadeOne.getReverseSetter() is not null
			rightColumn.getTable().addForeignKey(foreignKeyName, rightColumn, leftColumn);
			return new Duo<>(leftColumn, rightColumn);
		}
		
		private Column createOrUseReverseColumn(IEntityMappingStrategy<TRGT, TRGTID, ?> targetMappingStrategy,
												Column reverseColumn,
												ValueAccessPoint reverseGetter,
												AccessorDefinition accessorDefinition,
												ColumnNamingStrategy joinColumnNamingStrategy) {
			if (reverseColumn == null) {
				// no reverse column was given, so we look for the one mapped under the reverse getter
				reverseColumn = targetMappingStrategy.getPropertyToColumn().get(reverseGetter);
				if (reverseColumn == null) {
					// no column is defined under the getter, then we have to create one
					Set<Column> pkColumns = sourcePersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns();
					reverseColumn = targetMappingStrategy.getTargetTable().addColumn(
							joinColumnNamingStrategy.giveName(accessorDefinition),
							Iterables.first(pkColumns).getJavaType()
					);
				}
			}
			return reverseColumn;
		}
		
		@Override
		protected void addWriteCascades(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										RelationMode maintenanceMode) {
			boolean orphanRemoval = maintenanceMode == ALL_ORPHAN_REMOVAL;
			boolean writeAuthorized = maintenanceMode != READ_ONLY;
			if (writeAuthorized) {
				// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
				addInsertCascade(cascadeOne, targetPersister, sourcePersister);
				addUpdateCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
				addDeleteCascade(cascadeOne, targetPersister, sourcePersister, orphanRemoval);
			} else {
				// write is not authorized but we must maintain reverse column, so we'll create an update order for it
				
				// small class that helps locally to maintain update order and its values
				class ForeignKeyUpdateOrderProvider {
					
					private WriteOperation<UpwhereColumn<Table>> generateOrder() {
						PreparedUpdate<Table> tablePreparedUpdate = dialect.getDmlGenerator().buildUpdate(
								Arrays.asList((Column<Table, Object>) rightColumn),
								targetPersister.getMappingStrategy().getVersionedKeys());
						return new WriteOperation<>(tablePreparedUpdate,
								connectionConfiguration.getConnectionProvider());
					}
					
					private <C> void addValuesToUpdateBatch(Iterable<? extends C> entities,
															Function<C, SRCID> fkValueProvider,
															Function<C, SRC> sourceProvider,
															WriteOperation<UpwhereColumn<Table>> updateOrder) {
						entities.forEach(e -> {
							Map<UpwhereColumn<Table>, Object> values = new HashMap<>();
							values.put(new UpwhereColumn<>(rightColumn, true), fkValueProvider.apply(e));
							targetPersister.getMappingStrategy().getVersionedKeyValues(cascadeOne.getTargetProvider().get(sourceProvider.apply(e)))
									.forEach((c, o) -> values.put(new UpwhereColumn<>(c, false), o));
							updateOrder.addBatch(values);
						});
						
					}
				}
				
				ForeignKeyUpdateOrderProvider foreignKeyUpdateOrderProvider = new ForeignKeyUpdateOrderProvider();
				sourcePersister.getPersisterListener().addInsertListener(new InsertListener<SRC>() {
					
					/**
					 * Implemented to update target owning column after insert. Made AFTER insert to benefit from id when set by database with
					 * IdentifierPolicy is AFTER_INSERT
					 */
					@Override
					public void afterInsert(Iterable<? extends SRC> entities) {
						WriteOperation<UpwhereColumn<Table>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.generateOrder();
						foreignKeyUpdateOrderProvider.<SRC>addValuesToUpdateBatch(entities, sourcePersister::getId, Function.identity(), upwhereColumnWriteOperation);
						upwhereColumnWriteOperation.executeBatch();
					}
				});
				
				sourcePersister.getPersisterListener().addUpdateListener(new UpdateListener<SRC>() {
					
					@Override
					public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> entities, boolean allColumnsStatement) {
						WriteOperation<UpwhereColumn<Table>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.generateOrder();
						foreignKeyUpdateOrderProvider.<Duo<SRC, SRC>>addValuesToUpdateBatch((Iterable<? extends Duo<SRC, SRC>>) entities,
								duo -> sourcePersister.getId(duo.getLeft()),
								Duo::getLeft,
								upwhereColumnWriteOperation);
						foreignKeyUpdateOrderProvider.<Duo<SRC, SRC>>addValuesToUpdateBatch((Iterable<? extends Duo<SRC, SRC>>) entities,
								duo -> null,
								Duo::getRight,
								upwhereColumnWriteOperation);
						upwhereColumnWriteOperation.executeBatch();
						
					}
				});
				
				sourcePersister.getPersisterListener().addDeleteListener(new DeleteListener<SRC>() {
					
					/**
					 * Implemented to nullify target owning column before insert.
					 */
					@Override
					public void beforeDelete(Iterable<SRC> entities) {
						WriteOperation<UpwhereColumn<Table>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.generateOrder();
						foreignKeyUpdateOrderProvider.<SRC>addValuesToUpdateBatch(entities,
								duo -> null,
								Function.identity(),
								upwhereColumnWriteOperation);
						upwhereColumnWriteOperation.executeBatch();
						
					}
				});
			}
		}
		
		@Override
		protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(IReversibleAccessor<SRC, TRGT> targetAccessor) {
			return this.beanRelationFixer;
		}
		
		@Override
		protected void addInsertCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener) {
			super.addInsertCascade(cascadeOne, targetPersister, srcPersisterListener);
			// adding cascade treatment: after source insert, target is inserted
			Consumer<Iterable<? extends SRC>> persistTargetCascader = entities -> targetPersister.persist(
					Iterables.stream(entities).map(cascadeOne.getTargetProvider()::get).filter(Objects::nonNull).collect(Collectors.toList())
			);
			// Please note that 1st implementation was to simply add persistTargetCascader, but it invokes persist() which may invoke update()
			// and because we are in the relation-owned-by-target case targetPersister.update() needs foreign key value provider to be
			// fullfilled (see addUpdateCascade for addShadowColumnUpdate), so we wrap persistTargetCascader with a foreign key value provider.
			// This focuses particular use case when a target is modified and newly assigned to the source
			srcPersisterListener.addInsertListener(new InsertListener<SRC>() {
				/**
				 * Implemented to persist target instance after insert. Made AFTER insert to benefit from id when set by database with
				 * IdentifierPolicy is AFTER_INSERT
				 */
				@Override
				public void afterInsert(Iterable<? extends SRC> entities) {
					ThreadLocals.doWithThreadLocal(currentForeignKeyValueProvider, ForeignKeyValueProvider::new, (Consumer<ForeignKeyValueProvider>) fkValueProvider -> {
						for (SRC sourceEntity : entities) {
							currentForeignKeyValueProvider.get().add(cascadeOne.getTargetProvider().get(sourceEntity), sourceEntity);
						}
						persistTargetCascader.accept(entities);
					});
				}
			});
			targetPersister.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<>(rightColumn,
					trgt -> currentForeignKeyValueProvider.get().giveSourceId(trgt)));
		}
		
		@Override
		protected void addUpdateCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IEntityConfiguredJoinedTablesPersister<SRC, SRCID> srcPersisterListener,
										boolean orphanRemoval) {
			// if cascade is mandatory, then adding nullability checking before insert
			if (!cascadeOne.isNullable()) {
				srcPersisterListener.addUpdateListener(new MandatoryRelationCheckingBeforeUpdateListener<>(cascadeOne.getTargetProvider()));
			}
			// adding cascade treatment, please note that this will also be used by insert cascade if target is already persisted
			targetPersister.getMappingStrategy().addShadowColumnUpdate(new ShadowColumnValueProvider<>(rightColumn,
					trgt -> currentForeignKeyValueProvider.get().giveSourceId(trgt)));
			
			// - after source update, target is updated too
			srcPersisterListener.addUpdateListener(new UpdateListener<SRC>() {
				
				@Override
				public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
					if (currentForeignKeyValueProvider.get() == null) {
						currentForeignKeyValueProvider.set(new ForeignKeyValueProvider());
					}
					List<TRGT> newObjects = new ArrayList<>();
					List<Duo<TRGT, TRGT>> existingEntities = new ArrayList<>();
					
					// very small class to ease regetring entities to be persisted
					class PersisterHelper {
						
						void markToPersist(TRGT targetOfModified, SRC modifiedSource) {
							if (targetPersister.getMappingStrategy().isNew(targetOfModified)) {
								newObjects.add(targetOfModified);
							} else {
								existingEntities.add(new Duo<>(targetOfModified, null));
							}
							currentForeignKeyValueProvider.get().add(targetOfModified, modifiedSource);
						}
					}
					
					List<TRGT> nullifiedRelations = new ArrayList<>();
					PersisterHelper persisterHelper = new PersisterHelper(); 
					for (Duo<? extends SRC, ? extends SRC> payload : payloads) {
						
						TRGT targetOfModified = getTarget(payload.getLeft());
						TRGT targetOfUnmodified = getTarget(payload.getRight());
						if (targetOfModified == null && targetOfUnmodified != null) {
							// "REMOVED"
							// relation is nullified : relation column should be nullified too
							nullifiedRelations.add(targetOfUnmodified);
						} else if (targetOfModified != null && targetOfUnmodified == null) {
							// "ADDED"
							// newly set relation, entities will fully inserted / updated some lines above, nothing to do 
							// relation is set, we fully update modified bean, then its properties will be updated too
							persisterHelper.markToPersist(targetOfModified, payload.getLeft());
						} else {
							// "HELD"
							persisterHelper.markToPersist(targetOfModified, payload.getLeft());
							// Was target entity reassigned to another one ? Relation changed to another entity : we must nullify reverse column of detached target
							if (!targetPersister.getMappingStrategy().getId(targetOfUnmodified).equals(targetPersister.getMappingStrategy().getId(targetOfModified))) {
								nullifiedRelations.add(targetOfUnmodified);
							}
						}
					}
					
					targetPersister.insert(newObjects);
					targetPersister.update(existingEntities, allColumnsStatement);
					targetPersister.updateById(nullifiedRelations);
					
					currentForeignKeyValueProvider.remove();
				}
				
				private TRGT getTarget(SRC src) {
					return cascadeOne.getTargetProvider().get(src);
				}
				
				@Override
				public void onError(Iterable<? extends SRC> entities, RuntimeException runtimeException) {
					currentForeignKeyValueProvider.remove();
				}
			});
			if (orphanRemoval) {
				srcPersisterListener.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, cascadeOne.getTargetProvider()));
			}
		}
		
		@Override
		protected void addDeleteCascade(CascadeOne<SRC, TRGT, TRGTID> cascadeOne,
										IEntityConfiguredPersister<TRGT, TRGTID> targetPersister,
										IPersisterListener<SRC, SRCID> srcPersisterListener, boolean deleteTargetEntities) {
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
			
			private final CascadeOne<SRC, TRGT, TRGTID> cascadeOne;
			private final IEntityConfiguredPersister<TRGT, TRGTID> targetPersister;
			
			private NullifyRelationColumnBeforeDelete(CascadeOne<SRC, TRGT, TRGTID> cascadeOne, IEntityConfiguredPersister<TRGT, TRGTID> targetPersister) {
				this.cascadeOne = cascadeOne;
				this.targetPersister = targetPersister;
			}
			
			@Override
			public void beforeDelete(Iterable<SRC> entities) {
				ThreadLocals.doWithThreadLocal(currentForeignKeyValueProvider, ForeignKeyValueProvider::new, (Consumer<ForeignKeyValueProvider>) fkValueProvider ->
					this.targetPersister.updateById(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).peek(trgt ->
							fkValueProvider.add(trgt, null))
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
}
