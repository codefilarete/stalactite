package org.codefilarete.stalactite.engine.configurer.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class aimed at doing same thing as {@link CollectionUpdater} but for {@link Map} containing entities as keys :
 * requires to update {@link Map.Entry} as well as propagate insert / update /delete operation to key-entities. 
 *
 * @param <SRC> entity type owning the relation
 * @param <SRCID> entity owning the relation identifier type 
 * @param <K> Map key entity type
 * @param <V> Map value type
 * @param <ENTITY> entity type, expected to be K or V
 * @param <ENTITY_ID> entity type identifier
 * @param <KK> type of {@link KeyValueRecord} key when transforming initial Map entries to {@link KeyValueRecord} to be persisted
 * @param <VV> type of {@link KeyValueRecord} value when transforming initial Map entries to {@link KeyValueRecord} to be persisted
 * @author Guillaume Mary
 */
class MapUpdater<SRC, SRCID, K, V, ENTITY, ENTITY_ID, KK, VV> extends CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>> {
		
	private final EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> keyValueRecordPersister;
	
	private final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	
	private final ConfiguredRelationalPersister<ENTITY, ENTITY_ID> entityPersister;
	private final RelationMode maintenanceMode;
	private final Function<? super Entry<K, V>, ENTITY> entryBeanExtractor;
	private final BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KK, VV, SRCID>> recordBuilder;
	
	public MapUpdater(Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter,
					  ConfiguredRelationalPersister<ENTITY, ENTITY_ID> entityPersister,
					  EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> keyValueRecordPersister,
					  ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
					  RelationMode maintenanceMode,
					  Function<? super Entry<K, V>, ENTITY> entryBeanExtractor,
					  BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KK, VV, SRCID>> recordBuilder) {
		super(targetEntitiesGetter,
				new RelationalPersisterAsEntityWriter<>(entityPersister, entryBeanExtractor),
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				(Entry<K, V> entry) -> entityPersister.getId(entryBeanExtractor.apply(entry)));
		this.keyValueRecordPersister = keyValueRecordPersister;
		this.sourcePersister = sourcePersister;
		this.entityPersister = entityPersister;
		this.maintenanceMode = maintenanceMode;
		this.entryBeanExtractor = entryBeanExtractor;
		this.recordBuilder = recordBuilder;
	}
	
	@Override
	protected KeyValueAssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
		return new KeyValueAssociationTableUpdateContext(updatePayload);
	}
	
	@Override
	protected void onAddedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
		super.onAddedElements(updateContext, diff);
		KeyValueRecord<KK, VV, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance());
		((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(associationRecord);
	}
	
	@Override
	protected void onHeldElements(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
		super.onHeldElements(updateContext, diff);
		Duo<KeyValueRecord<KK, VV, SRCID>, KeyValueRecord<KK, VV, SRCID>> associationRecord = new Duo<>(
				newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance()),
				newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance())
		);
		((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated().add(associationRecord);
	}
	
	@Override
	protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
		super.onRemovedElements(updateContext, diff);
		
		KeyValueRecord<KK, VV, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance());
		((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(associationRecord);
	}
	
	@Override
	protected void insertTargets(UpdateContext updateContext) {
		// we insert association records after targets to satisfy integrity constraint
		if (maintenanceMode != RelationMode.READ_ONLY && maintenanceMode != RelationMode.ASSOCIATION_ONLY) {
			super.insertTargets(updateContext);
		}
		if (maintenanceMode != RelationMode.READ_ONLY) {
			super.insertTargets(updateContext);
			keyValueRecordPersister.insert(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted());
		}
	}
	
	@Override
	protected void updateTargets(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, boolean allColumnsStatement) {
		if (maintenanceMode != RelationMode.READ_ONLY && maintenanceMode != RelationMode.ASSOCIATION_ONLY) {
			super.updateTargets(updateContext, allColumnsStatement);
		}
		if (maintenanceMode != RelationMode.READ_ONLY) {
			keyValueRecordPersister.update(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated(), allColumnsStatement);
		}
	}
	
	@Override
	protected void deleteTargets(UpdateContext updateContext) {
		// we delete association records before targets to satisfy integrity constraint
		if (maintenanceMode != RelationMode.READ_ONLY) {
			keyValueRecordPersister.delete(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted());
		}
		if (maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			super.deleteTargets(updateContext);
		}
	}
	
	private KeyValueRecord<KK, VV, SRCID> newRecord(SRC e, Entry<K, V> record) {
		return recordBuilder.apply(record, sourcePersister.getId(e));
	}
	
	private static class RelationalPersisterAsEntityWriter<K, V, ENTITY, ENTITY_ID> implements EntityWriter<Entry<K, V>> {
		
		private final ConfiguredRelationalPersister<ENTITY, ENTITY_ID> valueEntityPersister;
		private final Function<? super Entry<K, V>, ENTITY> mapper;
		
		public RelationalPersisterAsEntityWriter(ConfiguredRelationalPersister<ENTITY, ENTITY_ID> valueEntityPersister, Function<? super Entry<K, V>, ENTITY> mapper) {
			this.valueEntityPersister = valueEntityPersister;
			this.mapper = mapper;
		}
		
		@Override
		public void update(Iterable<? extends Duo<Entry<K, V>, Entry<K, V>>> differencesIterable, boolean allColumnsStatement) {
			valueEntityPersister.update(Iterables.stream(differencesIterable)
					.map(duo -> new Duo<>(mapper.apply(duo.getLeft()), mapper.apply(duo.getRight())))
					.collect(Collectors.toSet()), allColumnsStatement);
		}
		
		@Override
		public void delete(Iterable<? extends Entry<K, V>> entities) {
			valueEntityPersister.delete(Iterables.stream(entities).map(mapper).collect(Collectors.toSet()));
		}
		
		@Override
		public void persist(Iterable<? extends Entry<K, V>> entities) {
			valueEntityPersister.persist(Iterables.stream(entities).map(mapper).collect(Collectors.toSet()));
		}
		
		@Override
		public boolean isNew(Entry<K, V> entity) {
			return valueEntityPersister.isNew(mapper.apply(entity));
		}
		
		@Override
		public void updateById(Iterable<? extends Entry<K, V>> entities) {
			valueEntityPersister.updateById(Iterables.stream(entities).map(mapper).collect(Collectors.toSet()));
		}
	}
	
	/**
	 * Dedicated context to Map update. Add storage of entries modifications, letting entities modifications management
	 * to the {@link UpdateContext} upper class.
	 * 
	 * @author Guillaume Mary
	 */
	class KeyValueAssociationTableUpdateContext extends UpdateContext {
		
		private final List<KeyValueRecord<KK, VV, SRCID>> associationRecordsToBeInserted = new ArrayList<>();
		private final List<Duo<KeyValueRecord<KK, VV, SRCID>, KeyValueRecord<KK, VV, SRCID>>> associationRecordsToBeUpdated = new ArrayList<>();
		private final List<KeyValueRecord<KK, VV, SRCID>> associationRecordsToBeDeleted = new ArrayList<>();
		
		public KeyValueAssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
			super(updatePayload);
		}
		
		public List<KeyValueRecord<KK, VV, SRCID>> getAssociationRecordsToBeInserted() {
			return associationRecordsToBeInserted;
		}
		
		public List<Duo<KeyValueRecord<KK, VV, SRCID>, KeyValueRecord<KK, VV, SRCID>>> getAssociationRecordsToBeUpdated() {
			return associationRecordsToBeUpdated;
		}
		
		public List<KeyValueRecord<KK, VV, SRCID>> getAssociationRecordsToBeDeleted() {
			return associationRecordsToBeDeleted;
		}
	}
}