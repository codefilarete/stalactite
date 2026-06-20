package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordIdMapping;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping;
import org.codefilarete.stalactite.engine.configurer.map.MapUpdater;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.CompositeMemberMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.EntryMemberMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.ScalarMemberMapping;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater.EntityWriter;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Functions.NullProofFunction;

import static org.codefilarete.stalactite.engine.configurer.map.MapUpdater.RelationalPersisterAsEntityWriter;
import static org.codefilarete.tool.Nullable.nullable;

public class EntryMapResolver {
	
	/**
	 * Combines several {@link EntityWriter} into a single one that fans out every write operation to each of them, in order.
	 * Used when both the key and the value of a {@link Map} are entities, so that a single updater cascades to both persisters
	 * (replacing the previously hand-written both-entities {@link EntityWriter}).
	 */
	public static <C> EntityWriter<C, ?> combine(Collection<? extends EntityWriter<C, ?>> writers) {
		return new CompositeEntityWriter<>(writers);
	}
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public EntryMapResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>,
			X, Y>
	KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> resolve(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                                       ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                                                       ConfiguredRelationalPersister<K, KID> keyEntityPersister,
	                                                       ConfiguredRelationalPersister<V, VID> valueEntityPersister) {
		
		KeyValueRecordMapping<X, Y, SRCID, MAPTABLE> relationRecordMapping = buildKeyValueRecordMapping(resolvedRelation, sourcePersister);
		
		KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> relationRecordPersister =
				new KeyValueRecordPersister<>(relationRecordMapping, dialect, connectionConfiguration);
		
		
		IdAccessor<SRC, SRCID> srcIdAccessor = sourcePersister.getMapping();
		
		boolean keyIsEntity = resolvedRelation.getKeyEntityDefinition() != null;
		boolean valueIsEntity = resolvedRelation.getValueEntityDefinition() != null;
		// The record key/value adapters turn a raw map key/value (K, V) into what is actually stored in the association
		// table (X, Y) : the entity id when that side is an entity, the raw value otherwise. They are shared by the update
		// cascade (below) and the insert/delete cascades (further down), and they make record building uniform across cases.
		Function<K, X> keyAdapter;
		List<EntityWriter<Entry<K, V>, ?>> entityWriters = new ArrayList<>(2);
		if (keyIsEntity) {
			// First, the target instances must be persisted to avoid foreign key errors.
			if (resolvedRelation.getKeyEntityDefinition().getRelationMode().allowsEntityWrite()) {
				registerTargetEntitiesInsertCascader(sourcePersister, keyEntityPersister, resolvedRelation.getAccessor(), Map::keySet);
			}
			keyAdapter = k -> (X) resolvedRelation.getKeyEntityDefinition().getEntity().getIdAccessor().get(k);
			entityWriters.add(new RelationalPersisterAsEntityWriter<>(keyEntityPersister, Entry::getKey, resolvedRelation.getKeyEntityDefinition().getRelationMode()));
		} else {
			keyAdapter = k -> (X) k;
		}
		
		Function<V, Y> valueAdapter;
		if (valueIsEntity) {
			if (resolvedRelation.getValueEntityDefinition().getRelationMode().allowsEntityWrite()) {
				registerTargetEntitiesInsertCascader(sourcePersister, valueEntityPersister, resolvedRelation.getAccessor(), Map::values);
			}
			valueAdapter = v -> (Y) resolvedRelation.getValueEntityDefinition().getEntity().getIdAccessor().get(v);
			entityWriters.add(new RelationalPersisterAsEntityWriter<>(valueEntityPersister, Entry::getValue, resolvedRelation.getValueEntityDefinition().getRelationMode()));
		} else {
			valueAdapter = v -> (Y) v;
		}
		
		// Footprint used to diff entries : it must rely on the entity side(s) so that, on a scalar side, a value change
		// becomes an association-record update, while on an entity side a change becomes a remove + add (so the newly
		// referenced entity gets inserted, preventing a foreign key violation). Hence :
		//  - only the key is an entity   -> key id
		//  - only the value is an entity -> value id
		//  - both are entities           -> the whole entry (entities expose an id-based equals())
		Accessor<Entry<K, V>, Object> entryFootprint = entry -> new Duo<>(keyAdapter.apply(entry.getKey()), valueAdapter.apply(entry.getValue()));
		
		// Update cascade for the entity side(s) : key, value or both. A single association table row carries both key and
		// value, so a single updater manages it for all three entity cases. The differences are abstracted away :
		//  - the cascade writer is the combination of the per-side writers (the both-entities case is therefore just the
		//    composition of the key writer and the value writer, no dedicated EntityWriter is needed anymore) ;
		//  - record building is uniform thanks to the key/value adapters above, so the same relationRecordPersister is used.
		// Only the diff footprint and the maintenance mode remain side-specific (see below).
		if (keyIsEntity || valueIsEntity) {
			EntityWriter<Entry<K, V>, ?> entityWriter = entityWriters.size() == 1
					? entityWriters.get(0)
					: combine(entityWriters);
			
			// The single association table is maintained according to the value side mode as soon as the value is an entity,
			// otherwise the key side one (this preserves the previous per-case behavior).
			CascadeOptions.RelationMode maintenanceMode = valueIsEntity
					? resolvedRelation.getValueEntityDefinition().getRelationMode()
					: resolvedRelation.getKeyEntityDefinition().getRelationMode();
			
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<X, Y, SRCID>> recordBuilder =
					(entry, srcId) -> new KeyValueRecord<>(srcId, keyAdapter.apply(entry.getKey()), valueAdapter.apply(entry.getValue()));
			
			Accessor<SRC, Set<Entry<K, V>>> entriesGetter = new NullProofFunction<>(resolvedRelation.getAccessor()::get).andThen(Map::entrySet)::apply;
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(entriesGetter, entityWriter,
					relationRecordPersister, sourcePersister, maintenanceMode, entryFootprint, recordBuilder);
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
		} else {
			// Fully scalar Map : no entity to cascade, a plain CollectionUpdater over the records is enough.
			KeyValueRecordPersister<K, V, SRCID, MAPTABLE> scalarRecordPersister = (KeyValueRecordPersister<K, V, SRCID, MAPTABLE>) relationRecordPersister;
			
			Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> collectionProviderAsPersistedInstances = src -> Iterables.collect(
					nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
					entry -> new KeyValueRecord<>(srcIdAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(true),
					HashSet::new);
			
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new CollectionUpdater<SRC, KeyValueRecord<K, V, SRCID>, Collection<KeyValueRecord<K, V, SRCID>>>(
					collectionProviderAsPersistedInstances,
					scalarRecordPersister,
					(o, i) -> { /* no reverse setter because we store only raw values */ },
					true,
					// we base our id policy on a particular identifier because Id is all the same for KeyValueRecord (it is source bean id)
					KeyValueRecord::footprint) {
				
				/**
				 * Overridden to force insertion of added entities because as a difference with default behavior (parent class), collection elements are
				 * not entities, so they can't be moved from a collection to another, hence they don't need to be updated, therefore there's no need to
				 * use {@code getElementPersister().persist(..)} mechanism. Even more : it is counterproductive (meaning false) because
				 * {@code persist(..)} uses {@code update(..)} when entities are considered already persisted (not {@code isNew()}), which is always the
				 * case for new {@link KeyValueRecord}
				 */
				@Override
				protected void insertTargets(UpdateContext updateContext) {
					scalarRecordPersister.insert(updateContext.getAddedElements());
				}
			};
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
		}
		
		if (resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY) {
			// We persist Map entries (key/value adapters are shared with the update cascade above)
			Accessor<SRC, Collection<KeyValueRecord<X, Y, SRCID>>> collectionProviderForInsert = toRecordCollectionProvider(resolvedRelation, srcIdAccessor, keyAdapter, valueAdapter, false);
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
			
			Accessor<SRC, Collection<KeyValueRecord<X, Y, SRCID>>> recordsProviderAsPersistedInstances = toRecordCollectionProvider(resolvedRelation, srcIdAccessor, keyAdapter, valueAdapter, true);
			sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
		}
		
		return relationRecordPersister;
	}
	
	/**
	 * Registers an insert cascade that persists the target entities (keys or values) of the relation {@link Map} before the
	 * source is inserted, in order to avoid foreign key violations.
	 *
	 * @param targetsExtractor extracts the entities to persist from the {@link Map} (typically {@code Map::keySet} or {@code Map::values})
	 * @param <T> target entity type (either the key type or the value type)
	 */
	private <SRC, K, V, T, M extends Map<K, V>> void registerTargetEntitiesInsertCascader(
			ConfiguredRelationalPersister<SRC, ?> sourcePersister,
			ConfiguredRelationalPersister<T, ?> targetPersister,
			Accessor<SRC, M> mapAccessor,
			Function<? super M, ? extends Collection<T>> targetsExtractor) {
		sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, T>(targetPersister) {
			@Override
			protected Collection<T> getTargets(SRC src) {
				List<T> targets = nullable(mapAccessor.get(src))
						.map(targetsExtractor)
						// - we force type to List<T> to let the getOr() call returns any kind of List, else we are stuck with ArrayList
						// - we copy the result to a List to avoid the Map being tempered with due to the remove(null), because keySet()/values() are backed by the Map
						.<List<T>>map(ArrayList::new)
						.getOr(Collections::emptyList);
				// Some Map implementations allow null key/value, so we remove them
				targets.remove(null);
				return targets;
			}
		});
	}
	
	// X is K, or KID if K is entity or not, composite or scalar
	// Y is V, or VID if V is entity or not, composite or scalar
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	KeyValueRecordMapping<X, Y, SRCID, MAPTABLE> buildKeyValueRecordMapping(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                                                        ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		IdentifierAssembler<SRCID, LEFTTABLE> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		
		MAPTABLE mapTable = resolvedRelation.getJoin().getRightKey().getTable();
		KeyValueRecordIdMapping<X, SRCID, MAPTABLE> idMapping;
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<X, Y, SRCID>, ?>, Column<MAPTABLE, ?>> propertiesMapping = new HashMap<>();
		
		EntryMemberMapping<V, MAPTABLE> valueEntityIdentifierMapping = resolvedRelation.getValueMapping();
		if (valueEntityIdentifierMapping instanceof ScalarMemberMapping) {
			Column<MAPTABLE, Y> valueColumn = ((ScalarMemberMapping<Y, MAPTABLE>) valueEntityIdentifierMapping).getColumn();
			propertiesMapping.put((ReadWritePropertyAccessPoint<KeyValueRecord<X, Y, SRCID>, Y>) (ReadWritePropertyAccessPoint) KeyValueRecord.VALUE_ACCESSOR, valueColumn);
		} else if (valueEntityIdentifierMapping instanceof CompositeMemberMapping) {
			CompositeMemberMapping<Y, MAPTABLE> compositeMemberMapping = (CompositeMemberMapping<Y, MAPTABLE>) valueEntityIdentifierMapping;
			Map<ReadWritePropertyAccessPoint<Y, ?>, Column<MAPTABLE, ?>> mapping = compositeMemberMapping.getMapping();
			
			Map<ReadWritePropertyAccessPoint<KeyValueRecord<X, Y, SRCID>, ?>, Column<MAPTABLE, ?>> x = chainWithRecordMemberAccessor(
					mapping,
					(ReadWritePropertyAccessPoint<KeyValueRecord<X, Y, SRCID>, Y>) (ReadWritePropertyAccessPoint) KeyValueRecord.VALUE_ACCESSOR,
					compositeMemberMapping.getBeanType());
			propertiesMapping.putAll(x);
		}
		
		
		if (resolvedRelation.getKeyMapping() instanceof ScalarMemberMapping) {
			Column<MAPTABLE, K> keyColumn = (Column<MAPTABLE, K>) ((ScalarMemberMapping<X, MAPTABLE>) resolvedRelation.getKeyMapping()).getColumn();
			KeyValueRecordIdMapping<K, SRCID, MAPTABLE> scalarMapping = new KeyValueRecordIdMapping<>(
					mapTable,
					columnedRow -> columnedRow.get(keyColumn),
					(Function<K, Map<Column<MAPTABLE, ?>, ?>>) k -> (Map) Maps.forHashMap(Column.class, Object.class).add(keyColumn, k),
					sourceIdentifierAssembler,
					resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
			// here, X is actually K because the entry key is a single value, not a composite one
			idMapping = (KeyValueRecordIdMapping<X, SRCID, MAPTABLE>) scalarMapping;
			
			propertiesMapping.put((ReadWritePropertyAccessPoint) KeyValueRecord.KEY_ACCESSOR, keyColumn);
		} else if (resolvedRelation.getKeyMapping() instanceof CompositeMemberMapping) {
			CompositeMemberMapping<KID, MAPTABLE> keyEntityIdentifierMapping = (CompositeMemberMapping<KID, MAPTABLE>) resolvedRelation.<KID>getKeyMapping();
			Class<KID> identifierType = keyEntityIdentifierMapping.getBeanType();
			EmbeddedClassMapping<KID, MAPTABLE> entityKeyMapping = new EmbeddedClassMapping<>(identifierType, mapTable, keyEntityIdentifierMapping.getMapping());
			KeyValueRecordIdMapping<KID, SRCID, MAPTABLE> compositeMapping = new KeyValueRecordIdMapping<>(
					mapTable,
					entityKeyMapping,
					sourceIdentifierAssembler,
					resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
			// here, X is actually KID because the entry key is a composite value, not a scalar one
			idMapping = (KeyValueRecordIdMapping<X, SRCID, MAPTABLE>) compositeMapping;
			
			propertiesMapping.putAll(chainWithRecordMemberAccessor(
					keyEntityIdentifierMapping.getMapping(),
					(ReadWritePropertyAccessPoint) KeyValueRecord.KEY_ACCESSOR,
					keyEntityIdentifierMapping.getBeanType()));
			
		} else {
			throw new IllegalArgumentException("Unsupported key entity identifier mapping : " + resolvedRelation.getKeyMapping());
		}
		return new KeyValueRecordMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				propertiesMapping,
				idMapping);
		
	}
	
	private static <SRC, SRCID, K, KK, V, VV, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Accessor<SRC, Collection<KeyValueRecord<KK, VV, SRCID>>> toRecordCollectionProvider(ResolvedMapRelation<SRC, SRCID, K, ?, V, ?, M, LEFTTABLE, MAPTABLE, ?, ?> resolvedRelation,
	                                                                                    IdAccessor<SRC, SRCID> idAccessor,
	                                                                                    Function<K, KK> keyAdapter,
	                                                                                    Function<V, VV> valueAdapter,
	                                                                                    boolean markAsPersisted) {
		
		return src -> Iterables.collect(
				nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
				// for now we consider X as K
				entry -> new KeyValueRecord<>(idAccessor.getId(src), keyAdapter.apply(entry.getKey()), valueAdapter.apply(entry.getValue())).setPersisted(markAsPersisted),
				HashSet::new);
	}
	
	private static class TargetInstancesInsertCascader<SRC, K, V, SRCID> extends AfterInsertCollectionCascader<SRC, KeyValueRecord<K, V, SRCID>> {
		
		private final Accessor<SRC, ? extends Collection<KeyValueRecord<K, V, SRCID>>> mapGetter;
		
		private TargetInstancesInsertCascader(EntityPersister<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> targetPersister,
		                                      Accessor<SRC, ? extends Collection<KeyValueRecord<K, V, SRCID>>> mapGetter) {
			super(targetPersister);
			this.mapGetter = mapGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister.
		}
		
		@Override
		protected Collection<KeyValueRecord<K, V, SRCID>> getTargets(SRC source) {
			return mapGetter.get(source);
		}
	}
	
	public static class KeyValueRecordPersister<K, V, SRCID, T extends Table<T>>
			extends SimpleRelationalEntityPersister<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, T> {
		
		public KeyValueRecordPersister(KeyValueRecordMapping<K, V, SRCID, T> keyValueRecordMapping,
		                               Dialect dialect,
		                               ConnectionConfiguration connectionConfiguration) {
			super(keyValueRecordMapping, dialect, connectionConfiguration);
		}
	}
	
	// X is either K or V
	private <X, MAPTABLE extends Table<MAPTABLE>, K, V, SRCID>
	Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> chainWithRecordMemberAccessor(
			Map<ReadWritePropertyAccessPoint<X, ?>, Column<MAPTABLE, ?>> propertyToColumn,
			PropertyAccessor<KeyValueRecord<K, V, SRCID>, X> recordMemberAccessor,
			Class<X> embeddedType) {
		Maps.ChainingMap<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> result = new Maps.ChainingMap<>();
		propertyToColumn.forEach((keyPropertyAccessor, column) -> {
			ReadWriteAccessorChain<KeyValueRecord<K, V, SRCID>, X, ?> key = new ReadWriteAccessorChain<>(recordMemberAccessor, keyPropertyAccessor);
			key.setNullValueHandler(new AccessorChain.ValueInitializerOnNullValue((accessor, inputType) -> {
				Class<?> memberType = accessor == recordMemberAccessor
						? embeddedType
						: inputType;
				return Reflections.newInstance(memberType);
			}));
			result.add(key, column);
		});
		return result;
	}
}

