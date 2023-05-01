package org.codefilarete.stalactite.engine.runtime.onetoone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.ColumnOptions.AfterInsertIdentifierPolicy;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteSupport;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedUpdate;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.stream;
import static org.codefilarete.tool.function.Predicates.not;

public class OneToOneOwnedByTargetEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> {
	
	/**
	 * Foreign key column value store, for insert, update and delete cases : stores parent entity value per child entity,
	 * (parent can be a null if child was removed from relation).
	 * Implemented as a ThreadLocal because we can hardly cross layers and methods to pass such a value.
	 * Cleaned after update and delete.
	 */
	private final ThreadLocal<TargetToSourceRelationStorage> currentTargetToSourceRelationStorage = new ThreadLocal<>();
	
	private class TargetToSourceRelationStorage {
		
		private final IdentityMap<TRGT, SRC> store = new IdentityMap<>();
		
		TargetToSourceRelationStorage() {
		}
		
		private void add(TRGT target, SRC source) {
			store.put(target, source);
		}
		
		private SRC get(TRGT target) {
			return store.get(target);
		}
		
		private SRCID giveSourceId(TRGT trgt) {
			return nullable(get(trgt)).map(sourcePersister.getMapping()::getId).get();
		}
	}
	
	public OneToOneOwnedByTargetEngine(ConfiguredPersister<SRC, SRCID> sourcePersister,
									   ConfiguredPersister<TRGT, TRGTID> targetPersister,
									   Accessor<SRC, TRGT> targetAccessor,
									   Map<Column<LEFTTABLE, Object>, Column<RIGHTTABLE, Object>> keyColumnsMapping) {
		super(sourcePersister, targetPersister, targetAccessor, keyColumnsMapping);
	}
	
	protected void ensureRelationStorageContext() {
		if (giveRelationStorageContext() == null) {
			currentTargetToSourceRelationStorage.set(new TargetToSourceRelationStorage());
		}
	}
	
	protected TargetToSourceRelationStorage giveRelationStorageContext() {
		return currentTargetToSourceRelationStorage.get();
	}
	
	protected void clearRelationStorageContext() {
		currentTargetToSourceRelationStorage.remove();
	}
	
	@Override
	public void addInsertCascade() {
		// adding cascade treatment: after source insert, target is persisted
		// Please note that we collect entities in a Set to avoid persisting duplicates twice which may produce constraint exception if some source
		// entities points to same target entity. In details the Set is an identity Set to avoid basing our comparison on implemented
		// equals/hashCode although this could be sufficient, identity seems safer and match our logic.
		Collector<TRGT, ?, Set<TRGT>> identitySetProvider = Collectors.toCollection(org.codefilarete.tool.collection.Collections::newIdentitySet);
		Consumer<Iterable<? extends SRC>> persistTargetCascader = entities -> {
			targetPersister.persist(stream(entities).map(targetAccessor::get).filter(Objects::nonNull).collect(identitySetProvider));
		};
		// Please note that 1st implementation was to simply add persistTargetCascader, but it invokes persist() which may invoke update()
		// and because we are in the relation-owned-by-target case targetPersister.update() needs foreign key value provider to be
		// fulfilled (see addUpdateCascade for addShadowColumnUpdate), so we wrap persistTargetCascader with a foreign key value provider.
		// This focuses particular use case when a target is modified and newly assigned to the source
		sourcePersister.addInsertListener(new InsertListener<SRC>() {
			/**
			 * Implemented to store target-to-source relation, made to help relation maintenance (because foreign key
			 * maintainer will refer to it) and avoid to depend on "mapped by" properties which is optional
			 * Made AFTER insert to benefit from id when set by database with IdentifierPolicy is {@link AfterInsertIdentifierPolicy}
			 */
			@Override
			public void afterInsert(Iterable<? extends SRC> entities) {
				ensureRelationStorageContext();
				for (SRC sourceEntity : entities) {
					giveRelationStorageContext().add(targetAccessor.get(sourceEntity), sourceEntity);
				}
				persistTargetCascader.accept(entities);
				clearRelationStorageContext();
			}
			
			@Override
			public void onInsertError(Iterable<? extends SRC> entities, RuntimeException runtimeException) {
				clearRelationStorageContext();
			}
		});
		
		targetPersister.<RIGHTTABLE>getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, RIGHTTABLE>() {
			@Override
			public Set<Column<RIGHTTABLE, Object>> getColumns() {
				return new HashSet<>(keyColumnsMapping.values());
			}
			
			@Override
			public Map<Column<RIGHTTABLE, Object>, Object> giveValue(TRGT bean) {
				// in many cases currentTargetToSourceRelationStorage is already present through source persister listener (insert or update)
				// but in the corner case of source and target persist same type (in a parent -> child case) then at very first insert of root
				// instance, currentTargetToSourceRelationStorage is not present, so we prevent this by initializing it 
				ensureRelationStorageContext();
				SRCID srcid = (SRCID) giveRelationStorageContext().giveSourceId(bean);
				Map<Column<LEFTTABLE, Object>, Object> columnValues = sourcePersister.getMapping().getIdMapping().<LEFTTABLE>getIdentifierAssembler().getColumnValues(srcid);
				return Maps.innerJoin(keyColumnsMapping, columnValues);
			}
		});
	}
	
	@Override
	public void addUpdateCascade(boolean orphanRemoval) {
		super.addUpdateCascade(orphanRemoval);
		// adding cascade treatment, please note that this will also be used by insert cascade if target is already persisted
		targetPersister.<RIGHTTABLE>getMapping().addShadowColumnUpdate(new ShadowColumnValueProvider<TRGT, RIGHTTABLE>() {
			@Override
			public Set<Column<RIGHTTABLE, Object>> getColumns() {
				return new HashSet<>(keyColumnsMapping.values());
			}
			
			@Override
			public Map<Column<RIGHTTABLE, Object>, Object> giveValue(TRGT bean) {
				// in many cases currentTargetToSourceRelationStorage is already present through source persister listener (insert or update)
				// but in the corner case of source and target persist same type (in a parent -> child case) then at very first insert of root
				// instance, currentTargetToSourceRelationStorage is not present, so we prevent this by initializing it 
				ensureRelationStorageContext();
				SRCID srcid = giveRelationStorageContext().giveSourceId(bean);
				Map<Column<LEFTTABLE, Object>, Object> columnValues = sourcePersister.getMapping().getIdMapping().<LEFTTABLE>getIdentifierAssembler().getColumnValues(srcid);
				return Maps.innerJoin(keyColumnsMapping, columnValues);
			}
		});
		
		// - after source update, target is updated too
		sourcePersister.addUpdateListener(new UpdateListener<SRC>() {
			
			@Override
			public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
				ensureRelationStorageContext();
				List<TRGT> newObjects = new ArrayList<>();
				List<Duo<TRGT, TRGT>> existingEntities = new ArrayList<>();
				
				// very small class to ease registering entities to be persisted
				class PersisterHelper {
					
					void markToPersist(TRGT targetOfModified, SRC modifiedSource) {
						if (targetPersister.getMapping().isNew(targetOfModified)) {
							newObjects.add(targetOfModified);
						} else {
							existingEntities.add(new Duo<>(targetOfModified, null));
						}
						giveRelationStorageContext().add(targetOfModified, modifiedSource);
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
					} else if (targetOfModified != null) {
						// "HELD"
						persisterHelper.markToPersist(targetOfModified, payload.getLeft());
						// Was target entity reassigned to another one ? Relation changed to another entity : we must nullify reverse column of detached target
						if (!targetPersister.getMapping().getId(targetOfUnmodified).equals(targetPersister.getMapping().getId(targetOfModified))) {
							nullifiedRelations.add(targetOfUnmodified);
						}
					} // else both sides are null => nothing to do
				}
				
				targetPersister.insert(newObjects);
				targetPersister.update(existingEntities, allColumnsStatement);
				if (!orphanRemoval) {
					targetPersister.updateById(nullifiedRelations);
				}
				// else : no need to nullify relation since entities are being deleted
				// (overall it fails since entities are already deleted through before delete listener)
				
				clearRelationStorageContext();
			}
			
			private TRGT getTarget(SRC src) {
				return src == null ? null : targetAccessor.get(src);
			}
			
			@Override
			public void onUpdateError(Iterable<? extends SRC> entities, RuntimeException runtimeException) {
				clearRelationStorageContext();
			}
		});
	}
	
	@Override
	public void addDeleteCascade(boolean orphanRemoval) {
		if (orphanRemoval) {
			// adding cascade treatment: target is deleted before source deletion (because of foreign key constraint)
			Predicate<TRGT> deletionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(not(targetPersister.getMapping().getIdMapping()::isNew));
			sourcePersister.addDeleteListener(new BeforeDeleteSupport<>(targetPersister::delete, targetAccessor::get, deletionPredicate));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(new BeforeDeleteByIdSupport<>(targetPersister::delete, targetAccessor::get, deletionPredicate));
		} else {
			// no target entities deletion asked (no delete orphan) : we only need to nullify the relation
			sourcePersister.addDeleteListener(new NullifyRelationColumnBeforeDelete(targetAccessor, targetPersister));
		}
	}
	
	/**
	 * Made to maintain reverse column when write is not authorized
	 * Will create an SQL update order dedicated to it
	 */
	public void addForeignKeyMaintainer(Dialect dialect, ConnectionConfiguration connectionConfiguration, Key<RIGHTTABLE, SRCID> rightKey) {
		ForeignKeyUpdateOrderProvider foreignKeyUpdateOrderProvider = new ForeignKeyUpdateOrderProvider(dialect, connectionConfiguration, rightKey);
		sourcePersister.getPersisterListener().addInsertListener(new InsertListener<SRC>() {
			
			/**
			 * Implemented to update target owning column after insert. Made AFTER insert to benefit from id when set by database with
			 * IdentifierPolicy is {@link AfterInsertIdentifierPolicy}
			 */
			@Override
			public void afterInsert(Iterable<? extends SRC> entities) {
				WriteOperation<UpwhereColumn<RIGHTTABLE>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.getOperation();
				foreignKeyUpdateOrderProvider.<SRC>addValuesToUpdateBatch(
						entities,
						sourcePersister::getId,
						sourcePersister.<LEFTTABLE>getMapping().getIdMapping().getIdentifierAssembler(),
						Function.identity(),
						upwhereColumnWriteOperation);
				upwhereColumnWriteOperation.executeBatch();
			}
		});
		
		sourcePersister.getPersisterListener().addUpdateListener(new UpdateListener<SRC>() {
			
			@Override
			public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> entities, boolean allColumnsStatement) {
				WriteOperation<UpwhereColumn<RIGHTTABLE>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.getOperation();
				foreignKeyUpdateOrderProvider.<Duo<SRC, SRC>>addValuesToUpdateBatch((Iterable<? extends Duo<SRC, SRC>>) entities,
						duo -> sourcePersister.getId(duo.getLeft()),
						sourcePersister.<RIGHTTABLE>getMapping().getIdMapping().getIdentifierAssembler(),
						Duo::getLeft,
						upwhereColumnWriteOperation);
				foreignKeyUpdateOrderProvider.<Duo<SRC, SRC>>addValuesToUpdateBatch((Iterable<? extends Duo<SRC, SRC>>) entities,
						duo -> null,
						sourcePersister.<RIGHTTABLE>getMapping().getIdMapping().getIdentifierAssembler(),
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
			public void beforeDelete(Iterable<? extends SRC> entities) {
				WriteOperation<UpwhereColumn<RIGHTTABLE>> upwhereColumnWriteOperation = foreignKeyUpdateOrderProvider.getOperation();
				foreignKeyUpdateOrderProvider.<SRC>addValuesToUpdateBatch(
						entities,
						duo -> null,	// nullifies the relation
						sourcePersister.<RIGHTTABLE>getMapping().getIdMapping().getIdentifierAssembler(),
						Function.identity(),
						upwhereColumnWriteOperation);
				upwhereColumnWriteOperation.executeBatch();
			}
		});
	}
	
	private class NullifyRelationColumnBeforeDelete implements DeleteListener<SRC> {
		
		private final ConfiguredPersister<TRGT, TRGTID> targetPersister;
		private final Accessor<SRC, TRGT> targetEntityProvider;
		
		private NullifyRelationColumnBeforeDelete(Accessor<SRC, TRGT> targetEntityProvider, ConfiguredPersister<TRGT, TRGTID> targetPersister) {
			this.targetPersister = targetPersister;
			this.targetEntityProvider = targetEntityProvider;
		}
		
		@Override
		public void beforeDelete(Iterable<? extends SRC> entities) {
			ensureRelationStorageContext();
			List<TRGT> targetEntities = stream(entities)
					.map(this::getTarget)
					.filter(Objects::nonNull)
					.peek(trgt -> giveRelationStorageContext().add(trgt, null))
					.collect(Collectors.toList());
			this.targetPersister.updateById(targetEntities);
			clearRelationStorageContext();
		}
		
		private TRGT getTarget(SRC src) {
			TRGT target = targetEntityProvider.get(src);
			// We only delete persisted instances (for logic and to prevent from non matching row count error)
			return target != null && !targetPersister.getMapping().getIdMapping().isNew(target) ? target : null;
		}
	}
	
	/**
	 * Small class that helps to maintain foreign key (reverse column), and only it
 	 */
	private class ForeignKeyUpdateOrderProvider {
		
		private final WriteOperation<UpwhereColumn<RIGHTTABLE>> foreignKeyUpdateOperation;
		
		public ForeignKeyUpdateOrderProvider(Dialect dialect, ConnectionConfiguration connectionConfiguration, Key<RIGHTTABLE, SRCID> rightKey) {
			PreparedUpdate<RIGHTTABLE> tablePreparedUpdate = dialect.getDmlGenerator().buildUpdate(
					(Set<Column<RIGHTTABLE, Object>>) rightKey.getColumns(),
					targetPersister.<RIGHTTABLE>getMapping().getVersionedKeys());
			foreignKeyUpdateOperation = dialect.getWriteOperationFactory().createInstance(tablePreparedUpdate,
					connectionConfiguration.getConnectionProvider());
		}
		
		private WriteOperation<UpwhereColumn<RIGHTTABLE>> getOperation() {
			return this.foreignKeyUpdateOperation;
		}
		
		private <C> void addValuesToUpdateBatch(Iterable<? extends C> entities,
												Function<C, SRCID> idProvider,
												IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler,
												Function<C, SRC> sourceProvider,
												WriteOperation<UpwhereColumn<RIGHTTABLE>> updateOrder) {
			Map<UpwhereColumn<RIGHTTABLE>, Object> values = new HashMap<>();
			entities.forEach(e -> {
				Map<Column<RIGHTTABLE, Object>, Object> columnValues = Maps.innerJoin(keyColumnsMapping, identifierAssembler.getColumnValues(idProvider.apply(e)));
				columnValues.forEach((key, value) -> values.put(new UpwhereColumn<>(key, true), value));
				targetPersister.<RIGHTTABLE>getMapping().getVersionedKeyValues(targetAccessor.get(sourceProvider.apply(e)))
						.forEach((c, o) -> values.put(new UpwhereColumn<>(c, false), o));
				updateOrder.addBatch(values);
				values.clear();
			});
		}
	}
}
