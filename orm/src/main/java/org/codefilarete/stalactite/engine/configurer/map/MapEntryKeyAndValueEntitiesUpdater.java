package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.StreamSplitter;

/**
 * Class aimed at doing same thing as {@link CollectionUpdater} but for {@link Map} containing entities as keys and values :
 * requires to update {@link Entry} as well as propagate insert / update /delete operation to key-entities and value-entities. 
 *
 * @param <SRC> entity type owning the relation
 * @param <SRCID> entity owning the relation identifier type 
 * @param <K> Map key entity type
 * @param <V> Map value entity type
 * @param <KK> type of {@link KeyValueRecord} key when transforming initial Map entries to {@link KeyValueRecord} to be persisted
 * @param <VV> type of {@link KeyValueRecord} value when transforming initial Map entries to {@link KeyValueRecord} to be persisted
 * @author Guillaume Mary
 */
class MapEntryKeyAndValueEntitiesUpdater<SRC, SRCID, K, V, KK, VV> extends CollectionUpdater<SRC, KeyValueRecord<K, V, SRCID>, Set<KeyValueRecord<K, V, SRCID>>> {
	
	private final ConfiguredRelationalPersister<K, ?> keyEntityPersister;
	private final ConfiguredRelationalPersister<V, ?> valueEntityPersister;
	private final RelationMode keyEntityMaintenanceMode;
	private final RelationMode valueEntityMaintenanceMode;
	private final boolean associationRecordWritable;
	
	public MapEntryKeyAndValueEntitiesUpdater(Function<SRC, Set<KeyValueRecord<K, V, SRCID>>> targetEntityGetter,
											  Function<K, KK> keyMapper,
											  Function<V, VV> valueMapper,
											  ConfiguredRelationalPersister<K, ?> keyEntityPersister,
											  ConfiguredRelationalPersister<V, ?> valueEntityPersister,
											  EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> keyValueRecordPersister,
											  RelationMode keyMaintenanceMode,
											  RelationMode valueMaintenanceMode) {
		super(targetEntityGetter,
				new RelationalPersisterAsEntityWriter<>(keyValueRecordPersister, keyMapper, valueMapper),
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// Notice that the way this computation is done as a huge impact on what's done in the updateTargets(..) method
				(KeyValueRecord<K, V, SRCID> entry) -> {
					// We don't take value into account to trigger updateTargets(..) : if it wa taken into account then
					// we would have only insert and delete actions on association records, which leads to not optimal
					// database actions.
					int result = entry.getId().getId().hashCode();
					result = 31 * result + keyEntityPersister.getId(entry.getKey()).hashCode();
					return result;
				});
		this.keyEntityPersister = keyEntityPersister;
		this.valueEntityPersister = valueEntityPersister;
		this.keyEntityMaintenanceMode = keyMaintenanceMode;
		this.valueEntityMaintenanceMode = valueMaintenanceMode;
		this.associationRecordWritable = this.keyEntityMaintenanceMode != RelationMode.READ_ONLY;
	}
	
	@Override
	protected void insertTargets(UpdateContext updateContext) {
		if (keyEntityMaintenanceMode == RelationMode.ALL || keyEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			keyEntityPersister.insert(updateContext.getAddedElements().stream().map(KeyValueRecord::getKey).collect(Collectors.toSet()));
		}
		if (valueEntityMaintenanceMode == RelationMode.ALL || valueEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			valueEntityPersister.insert(updateContext.getAddedElements().stream().map(KeyValueRecord::getValue).collect(Collectors.toSet()));
		}
		// we insert association records after targets to satisfy integrity constraint
		if (associationRecordWritable) {
			super.insertTargets(updateContext);
		}
	}
	
	@Override
	protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
		// Here we only manage entry values because keys are not modified since they are part of the primary key and overall
		// are in CollectionUpdate identifier (see constructor) so they are already managed through insertTargets(..) and deleteTargets(..)
		// Hence code below focuses only on value removal, update and addition
		if (valueEntityMaintenanceMode != RelationMode.READ_ONLY) {
			Set<V> removedValues = new KeepOrderSet<>();
			Set<V> addedValues = new KeepOrderSet<>();
			Set<AbstractDiff<KeyValueRecord<K, V, SRCID>>> modifiedValues = new KeepOrderSet<>();
			new StreamSplitter<>(updateContext.getHeldElements().stream())
					.dispatch(diff -> diff.getReplacingInstance() == null, entry -> {
						removedValues.add(entry.getSourceInstance().getValue());
					})
					.dispatch(diff -> diff.getReplacingInstance() != null, entry -> {
						if (!valueEntityPersister.getId(entry.getSourceInstance().getValue())
								.equals(valueEntityPersister.getId(entry.getReplacingInstance().getValue()))) {
							modifiedValues.add(entry);
							addedValues.add(entry.getReplacingInstance().getValue());
							removedValues.add(entry.getSourceInstance().getValue());
						}
					})
					.split();
			
			if (valueEntityMaintenanceMode == RelationMode.ALL || valueEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
				valueEntityPersister.persist(addedValues);
			}
			
			Set<Duo<KeyValueRecord<K, V, SRCID>, KeyValueRecord<K, V, SRCID>>> recordsToBeUpdated = modifiedValues.stream()
					.map(diff -> new Duo<>(diff.getReplacingInstance(), diff.getSourceInstance())).collect(Collectors.toSet());
			elementPersister.update(recordsToBeUpdated, allColumnsStatement);
			
			if (valueEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
				valueEntityPersister.delete(removedValues);
			}
		}
	}
	
	@Override
	protected void deleteTargets(UpdateContext updateContext) {
		// we delete association records before targets to satisfy integrity constraint
		if (associationRecordWritable) {
			super.deleteTargets(updateContext);
		}
		if (keyEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			keyEntityPersister.delete(updateContext.getRemovedElements().stream().map(KeyValueRecord::getKey).collect(Collectors.toSet()));
		}
		if (valueEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			valueEntityPersister.delete(updateContext.getRemovedElements().stream().map(KeyValueRecord::getValue).collect(Collectors.toSet()));
		}
	}
	
	/**
	 * Class that redirects {@link EntityWriter} methods to a given {@link EntityPersister}
	 * @param <K>
	 * @param <V>
	 * @param <KK>
	 * @param <VV>
	 * @param <SRCID>
	 * @author Guillaume Mary
	 */
	private static class RelationalPersisterAsEntityWriter<K, V, KK, VV, SRCID> implements EntityWriter<KeyValueRecord<K, V, SRCID>> {
		
		private final EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> relationEntityPersister;
		private final Function<K, KK> keyMapper;
		private final Function<V, VV> valueMapper;
		
		public RelationalPersisterAsEntityWriter(EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> relationEntityPersister,
												 Function<K, KK> keyMapper,
												 Function<V, VV> valueMapper) {
			this.relationEntityPersister = relationEntityPersister;
			this.keyMapper = keyMapper;
			this.valueMapper = valueMapper;
		}
		
		@Override
		public void update(Iterable<? extends Duo<KeyValueRecord<K, V, SRCID>, KeyValueRecord<K, V, SRCID>>> differencesIterable, boolean allColumnsStatement) {
			relationEntityPersister.update(Iterables.stream(differencesIterable)
					.map(keyValueRecordKeyValueRecordDuo -> {
						KeyValueRecord<K, V, SRCID> left = keyValueRecordKeyValueRecordDuo.getLeft();
						KeyValueRecord<K, V, SRCID> right = keyValueRecordKeyValueRecordDuo.getRight();
						return new Duo<>(
								new KeyValueRecord<>(left.getId().getId(),
										keyMapper.apply(left.getKey()), valueMapper.apply(left.getValue())),
								new KeyValueRecord<>(right.getId().getId(),
										keyMapper.apply(right.getKey()), valueMapper.apply(right.getValue())));
					})
					.collect(Collectors.toSet()), allColumnsStatement);
		}
		
		@Override
		public void delete(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			relationEntityPersister.delete(Iterables.stream(entities)
					.map(entity -> new KeyValueRecord<>(entity.getId().getId(),
							keyMapper.apply(entity.getKey()), valueMapper.apply(entity.getValue())))
					.collect(Collectors.toSet()));
		}
		
		@Override
		public void insert(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			relationEntityPersister.insert(Iterables.stream(entities)
					.map(entity -> new KeyValueRecord<>(entity.getId().getId(),
							keyMapper.apply(entity.getKey()), valueMapper.apply(entity.getValue())))
					.collect(Collectors.toSet()));
		}
		
		@Override
		public void persist(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			relationEntityPersister.persist(Iterables.stream(entities)
					.map(entity -> new KeyValueRecord<>(entity.getId().getId(),
							keyMapper.apply(entity.getKey()), valueMapper.apply(entity.getValue())))
					.collect(Collectors.toSet()));
		}
		
		@Override
		public void updateById(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			relationEntityPersister.updateById(Iterables.stream(entities)
					.map(entity -> new KeyValueRecord<>(entity.getId().getId(),
							keyMapper.apply(entity.getKey()), valueMapper.apply(entity.getValue())))
					.collect(Collectors.toSet()));
		}
		
		@Override
		public boolean isNew(KeyValueRecord<K, V, SRCID> entity) {
			// all records are persisted (not new) since we've just build them from database above
			return false;
		}
	}
}