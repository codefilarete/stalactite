package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.dsl.idpolicy.GeneratedKeysPolicy;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	extends AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S>
{
	
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
	
	public OneToManyWithMappedAssociationEngine(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister,
	                                            MappedManyRelationDescriptor<SRC, TRGT, S, SRCID, RIGHTTABLE> manyRelationDescriptor,
	                                            EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                                            Set<Column<RIGHTTABLE, ?>> mappedReverseColumns,
	                                            Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider) {
		super(sourcePersister, targetPersister, manyRelationDescriptor);
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
	
	@Override
	public MappedManyRelationDescriptor<SRC, TRGT, S, SRCID, RIGHTTABLE> getManyRelationDescriptor() {
		return (MappedManyRelationDescriptor<SRC, TRGT, S, SRCID, RIGHTTABLE>) manyRelationDescriptor;
	}
	
	@Override
	public void addInsertCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister) {
		sourcePersister.addInsertListener(
				new TargetInstancesInsertCascader(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
	}
	
	@Override
	public void addUpdateCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister) {
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
		addTargetInstancesUpdateCascader(getManyRelationDescriptor().isOrphanRemoval());
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
		Mutator<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<>(
				manyRelationDescriptor.getCollectionAccessPoint(),
				targetPersister,
				manyRelationDescriptor.getReverseSetter(),
				shouldDeleteRemoved);
		sourcePersister.addUpdateListener(
				new AfterUpdateTrigger<>(collectionUpdater));
	}
	
	@Override
	public void addDeleteCascade(EntityWriteExecutor<TRGT, TRGTID> targetPersister) {
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
		
		if (getManyRelationDescriptor().isOrphanRemoval()) {
			// adding deletion of many-side entities
			sourcePersister.addDeleteListener(
					new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(
					new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDescriptor.getCollectionAccessPoint()));
		} else // entity shouldn't be deleted, so we may have to update it
			if (manyRelationDescriptor.getReverseSetter() != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, TRGT>(targetPersister) {
					
					@Override
					public void beforeDelete(Iterable<? extends SRC> entities) {
						List<TRGT> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> manyRelationDescriptor.getReverseSetter().set(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<TRGT> getTargets(SRC src) {
						return nullable(manyRelationDescriptor.getCollectionAccessPoint().get(src)).getOr(manyRelationDescriptor.getCollectionFactory());
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
	
	public class TargetInstancesInsertCascader extends AfterInsertCollectionCascader<SRC, TRGT> {
		
		private final Accessor<SRC, ? extends Collection<TRGT>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister, Accessor<SRC, ? extends Collection<TRGT>> collectionGetter) {
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
			return collectionGetter.get(source);
		}
	}
	
	/**
	 * Triggers given consumer after update
	 * 
	 * @param <I>
	 */
	public static class AfterUpdateTrigger<I> implements UpdateListener<I> {
		
		private final Mutator<Duo<? extends I, ? extends I>, Boolean> afterUpdateListener;
		
		public AfterUpdateTrigger(Mutator<? extends Duo<? extends I, ? extends I>, Boolean> afterUpdateListener) {
			this.afterUpdateListener = (Mutator<Duo<? extends I, ? extends I>, Boolean>) afterUpdateListener;
		}
		
		@Override
		public void afterUpdate(Iterable<? extends Duo<I, I>> entities, boolean allColumnsStatement) {
			entities.forEach(entry -> afterUpdateListener.set(entry, allColumnsStatement));
		}
	}
	
	public static class DeleteTargetEntitiesBeforeDeleteCascader<I, O> extends BeforeDeleteCollectionCascader<I, O> {
		
		private final Accessor<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteTargetEntitiesBeforeDeleteCascader(EntityWriteExecutor<O, ?> targetPersister, Accessor<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			return collectionGetter.get(i);
		}
	}
	
	public static class DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<I, O> extends BeforeDeleteByIdCollectionCascader<I, O> {
		
		private final Accessor<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteByIdTargetEntitiesBeforeDeleteByIdCascader(EntityWriteExecutor<O, ?> targetPersister,
																Accessor<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetDelete(Iterable<O> entities) {
			// no post treatment to do
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			return collectionGetter.get(i);
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
			S collection = manyRelationDescriptor.getCollectionAccessPoint().get(sourceEntity);
			nullable(collection).getOr(manyRelationDescriptor.getCollectionFactory()).forEach(trgt -> {
				giveRelationStorageContext().add(trgt, relationIsNullified ? null : sourceEntity);
			});
		}
	}
}
