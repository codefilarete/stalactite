package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnOptions.GeneratedKeysPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismPersister;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	protected final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	
	protected final ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister;
	
	protected final MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> manyRelationDescriptor;
	
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	/**
	 * Foreign key column value store, for insert, update and delete cases : stores parent entity value per child entity,
	 * (parent can be a null if child was removed from relation).
	 * Implemented as a ThreadLocal because we can hardly cross layers and methods to pass such a value.
	 * Cleaned after update and delete.
	 */
	protected final ThreadLocal<TargetToSourceRelationStorage> currentTargetToSourceRelationStorage = new ThreadLocal<>();
	
	/**
	 * Storage of relation between TRGT and SRC entities to avoid to depend on "mapped by" properties which is optional.
	 * Foreign key maintenance code will refer to it.
	 * @author Guillaume Mary
	 */
	protected class TargetToSourceRelationStorage {
		
		private final IdentityMap<TRGT, SRC> store = new IdentityMap<>();
		
		TargetToSourceRelationStorage() {
		}
		
		private void add(TRGT target, SRC source) {
			store.put(target, source);
		}
		
		protected SRC get(TRGT target) {
			return store.get(target);
		}
		
		private SRCID giveSourceId(TRGT trgt) {
			return nullable(get(trgt)).map(sourcePersister.getMapping()::getId).get();
		}
	}
	
	protected final ShadowColumnValueProvider<TRGT, RIGHTTABLE> foreignKeyValueProvider;
	
	public OneToManyWithMappedAssociationEngine(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
												MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> manyRelationDescriptor,
												ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
												Set<Column<RIGHTTABLE, ?>> mappedReverseColumns,
												Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider,
												ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.sourcePersister = sourcePersister;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.foreignKeyValueProvider = new ShadowColumnValueProvider<TRGT, RIGHTTABLE>() {
			@Override
			public Set<Column<RIGHTTABLE, ?>> getColumns() {
				return mappedReverseColumns;
			}
			
			@Override
			public Map<Column<RIGHTTABLE, ?>, ?> giveValue(TRGT trgt) {
				Map<Column<RIGHTTABLE, ?>, Object> result;
				if (giveRelationStorageContext() == null) {
					// case of TRGT is also root (SRC) in a cycling parent -> parent relation : when some root entities are
					// inserted/updated the insert listener that initializes currentForeignKeyValueProvider on databaseAutoIncrement is not yet called
					result = new HashMap<>();
					getColumns().forEach(col -> result.put(col, null));
				} else {
					SRCID srcid = giveRelationStorageContext().giveSourceId(trgt);
					result = (Map<Column<RIGHTTABLE, ?>, Object>) reverseColumnsValueProvider.apply(srcid);
				}
				return result;
			}
		};
		
		addForeignKeyManager();
	}
	
	
	protected TargetToSourceRelationStorage giveRelationStorageContext() {
		return currentTargetToSourceRelationStorage.get();
	}
	
	protected void clearRelationStorageContext() {
		currentTargetToSourceRelationStorage.remove();
	}
	
	protected void addForeignKeyManager() {
		targetPersister.<RIGHTTABLE>getMapping().addShadowColumnInsert(foreignKeyValueProvider);
		targetPersister.<RIGHTTABLE>getMapping().addShadowColumnUpdate(foreignKeyValueProvider);
	}
	
	public MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> getManyRelationDescriptor() {
		return manyRelationDescriptor;
	}
	
	public <LEFTTABLE extends Table<LEFTTABLE>, JOINTYPE> void propagateMappedAssociationToSubTables(Key<LEFTTABLE, JOINTYPE> foreignKey) {
		// adding foreign key constraint
		// NB: we ask it to targetPersister because it may be polymorphic or complex (ie contains several tables) so it knows better how to do it
		AbstractPolymorphismPersister<?, ?> targetPersisterAsPolymorphic = AbstractPolymorphismPersister.lookupForPolymorphicPersister(targetPersister);
		if (targetPersisterAsPolymorphic != null) {
			targetPersisterAsPolymorphic.propagateMappedAssociationToSubTables(foreignKey, sourcePersister.getMainTable().getPrimaryKey(), foreignKeyNamingStrategy::giveName);
		} else {
			AbstractPolymorphismPersister<?, ?> sourcePersisterAsPolymorphic = AbstractPolymorphismPersister.lookupForPolymorphicPersister(sourcePersister);
			if (!(sourcePersisterAsPolymorphic instanceof TablePerClassPolymorphismPersister)) {
				// here source persister is either a simple one, or a join-table or a single-table
				sourcePersister.<LEFTTABLE>getMainTable().addForeignKey(foreignKeyNamingStrategy::giveName,
						foreignKey, sourcePersister.getMainTable().getPrimaryKey());
			}   // else source persister is table-per-class: FK should be from right side (owner) to the several left sides, which is not possible
				// because database vendors don't allow adding several FK on same source columns to different targets
		}
	}
	
	public <T1 extends Table<T1>, T2 extends Table<T2>> String addSelectCascade(Key<T1, SRCID> sourcePrimaryKey,
																				boolean loadSeparately) {
		// we add target subgraph joins to main persister
		String relationJoinNodeName = targetPersister.joinAsMany(ROOT_JOIN_NAME, sourcePersister, manyRelationDescriptor.getCollectionProvider(), sourcePrimaryKey, (Key<T2, SRCID>) manyRelationDescriptor.getReverseColumn(),
				manyRelationDescriptor.getRelationFixer(), null, true, loadSeparately);
		
		// we must trigger subgraph event on loading of our own graph, this is mainly for event that initializes things because given ids
		// are not those of their entity
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
		return relationJoinNodeName;
	}
	
	public void addInsertCascade(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		sourcePersister.addInsertListener(
				new TargetInstancesInsertCascader(targetPersister, manyRelationDescriptor.getCollectionGetter()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		sourcePersister.addUpdateListener(new UpdateListener<SRC>() {
			
			/**
			 * Implemented to store target-to-source relation, made to help relation maintenance (because foreign key
			 * maintainer will refer to it) and avoid to depend on "mapped by" properties which is optional
			 * Made AFTER insert to benefit from id when set by database with IdentifierPolicy is {@link GeneratedKeysPolicy}
			 */
			@Override
			public void afterUpdate(Iterable<? extends Duo<SRC, SRC>> entities, boolean allColumnsStatement) {
				storeTargetToSourceRelation(Iterables.mappingIterator(entities, Duo::getLeft), false);
			}
			
			/**
			 * Overridden to help debug
			 * @return targetToSourceRelationStorer
			 */
			@Override
			public String toString() {
				return "targetToSourceRelationStorer";
			}
		});
		addTargetInstancesUpdateCascader(shouldDeleteRemoved);
		sourcePersister.addUpdateListener(new UpdateListener<SRC>() {
			@Override
			public void afterUpdate(Iterable<? extends Duo<SRC, SRC>> entities, boolean allColumnsStatement) {
				clearRelationStorageContext();
			}
			
			/**
			 * Overridden to help debug
			 * @return targetToSourceRelationStorageCleaner
			 */
			@Override
			public String toString() {
				return "targetToSourceRelationStorageCleaner";
			}
		});
	}
	
	protected void addTargetInstancesUpdateCascader(boolean shouldDeleteRemoved) {
		BiConsumer<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<>(
				manyRelationDescriptor.getCollectionGetter(),
				targetPersister,
				manyRelationDescriptor.getReverseSetter(),
				shouldDeleteRemoved);
		sourcePersister.addUpdateListener(
				new AfterUpdateTrigger<>(collectionUpdater));
	}
	
	public void addDeleteCascade(boolean shouldDeleteRemoved, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		sourcePersister.addDeleteListener(new DeleteListener<SRC>() {
			
			@Override
			public void beforeDelete(Iterable<? extends SRC> entities) {
				storeTargetToSourceRelation(entities, true);
			}
		});
		sourcePersister.addDeleteByIdListener(new DeleteByIdListener<SRC>() {
			@Override
			public void beforeDeleteById(Iterable<? extends SRC> entities) {
				storeTargetToSourceRelation(entities, true);
			}
		});
		
		if (shouldDeleteRemoved) {
			// adding deletion of many-side entities
			sourcePersister.addDeleteListener(
					new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(
					new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		} else // entity shouldn't be deleted, so we may have to update it
			if (manyRelationDescriptor.getReverseSetter() != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, TRGT>(targetPersister) {
					
					@Override
					public void beforeDelete(Iterable<? extends SRC> entities) {
						List<TRGT> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> manyRelationDescriptor.getReverseSetter().accept(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<TRGT> getTargets(SRC src) {
						return nullable(manyRelationDescriptor.getCollectionGetter().apply(src)).getOr(manyRelationDescriptor.getCollectionFactory());
					}
				});
			}
		sourcePersister.addDeleteListener(new DeleteListener<SRC>() {
			@Override
			public void afterDelete(Iterable<? extends SRC> entities) {
				clearRelationStorageContext();
			}
		});
		sourcePersister.addDeleteByIdListener(new DeleteByIdListener<SRC>() {
			@Override
			public void afterDeleteById(Iterable<? extends SRC> entities) {
				clearRelationStorageContext();
			}
		});
	}
	
	/**
	 * Method to be invoked in case of entity cycle detected in its persistence configuration.
	 * We add a second phase load because cycle can hardly be supported by simply joining things together, in particular due to that
	 * Query and SQL generation don't support several instances of table and columns in them (aliases generation must be enhanced), and
	 * overall column reading will be messed up because of that (to avoid all of this we should have mapping strategy clones)
	 * 				
	 * @param sourcePrimaryKey left table primary key
	 * @param relationOwner right table primary key
	 * @param collectionGetter relation provider
	 * @param firstPhaseCycleLoadListener code to be invoked when reading rows
	 */
	public <T extends Table<T>> void addSelectIn2Phases(PrimaryKey<T, SRCID> sourcePrimaryKey,
														Key<T, SRCID> relationOwner,
														ReversibleAccessor<SRC, C> collectionGetter,
														FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		// Join is declared on non-added tables : Person (alias = null) / Person (alias = null)
		T relationOwnerTable = sourcePrimaryKey.getTable();
		Table relationOwnerTableClone = new Table(relationOwnerTable.getName());
		KeyBuilder<?, SRCID> relationOwnerPrimaryKeyBuilder = Key.from(relationOwnerTableClone);
		relationOwnerTable.getPrimaryKey().getColumns().forEach(column ->
				relationOwnerPrimaryKeyBuilder.addColumn(relationOwnerTableClone.addColumn(column.getName(), column.getJavaType()))
		);
		KeyBuilder<Table, SRCID> relationOwnerClone = Key.from(relationOwnerTableClone);
		relationOwner.getColumns().forEach(column ->
				relationOwnerClone.addColumn(relationOwnerTableClone.addColumn(column.getExpression(), column.getJavaType()))
		);
		
		IdentifierAssembler<TRGTID, T> targetIdentifierAssembler = targetPersister.getMapping().getIdMapping().getIdentifierAssembler();
		Key<Table, SRCID> rightKey = relationOwnerClone.build();
		Key<?, SRCID> rightPrimaryKey = relationOwnerPrimaryKeyBuilder.build();
		sourcePersister.getEntityJoinTree().addPassiveJoin(
				ROOT_JOIN_NAME,
				sourcePrimaryKey,
				rightKey,
				relationOwnerTableClone.getName() + "_" + AccessorDefinition.giveDefinition(collectionGetter).getName(),
				JoinType.OUTER,
				rightPrimaryKey.getColumns(),
				(src, columnValueProvider) -> firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, targetIdentifierAssembler.assemble(columnValueProvider)),
				true);
	}
	
	public class TargetInstancesInsertCascader extends AfterInsertCollectionCascader<SRC, TRGT> {
		
		private final Function<SRC, ? extends Collection<TRGT>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<TRGT, TRGTID> targetPersister, Function<SRC, ? extends Collection<TRGT>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		public void afterInsert(Iterable<? extends SRC> entities) {
			storeTargetToSourceRelation(entities, false);
			super.afterInsert(entities);
			clearRelationStorageContext();
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends TRGT> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<TRGT> getTargets(SRC source) {
			return collectionGetter.apply(source);
		}
	}
	
	/**
	 * Triggers given consumer after update
	 * 
	 * @param <I>
	 */
	public static class AfterUpdateTrigger<I> implements UpdateListener<I> {
		
		private final BiConsumer<Duo<? extends I, ? extends I>, Boolean> afterUpdateListener;
		
		public AfterUpdateTrigger(BiConsumer<? extends Duo<? extends I, ? extends I>, Boolean> afterUpdateListener) {
			this.afterUpdateListener = (BiConsumer<Duo<? extends I, ? extends I>, Boolean>) afterUpdateListener;
		}
		
		@Override
		public void afterUpdate(Iterable<? extends Duo<I, I>> entities, boolean allColumnsStatement) {
			entities.forEach(entry -> afterUpdateListener.accept(entry, allColumnsStatement));
		}
	}
	
	public static class DeleteTargetEntitiesBeforeDeleteCascader<I, O> extends BeforeDeleteCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteTargetEntitiesBeforeDeleteCascader(EntityPersister<O, ?> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			return collectionGetter.apply(i);
		}
	}
	
	public static class DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<I, O> extends BeforeDeleteByIdCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteByIdTargetEntitiesBeforeDeleteByIdCascader(EntityPersister<O, ?> targetPersister,
																Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetDelete(Iterable<O> entities) {
			// no post treatment to do
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			return collectionGetter.apply(i);
		}
	}
	
	/**
	 * Store target-to-source relation in current thread storage
	 */
	private void storeTargetToSourceRelation(Iterable<? extends SRC> sourceEntities, boolean relationIsNullified) {
		if (giveRelationStorageContext() == null) {
			currentTargetToSourceRelationStorage.set(new TargetToSourceRelationStorage());
		}
		for (SRC sourceEntity : sourceEntities) {
			C collection = manyRelationDescriptor.getCollectionGetter().apply(sourceEntity);
			nullable(collection).getOr(manyRelationDescriptor.getCollectionFactory()).forEach(trgt -> {
				giveRelationStorageContext().add(trgt, relationIsNullified ? null : sourceEntity);
			});
		}
	}
}
