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
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
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
import org.codefilarete.tool.function.Functions;

import static org.codefilarete.tool.Nullable.nullable;

public class EntryMapResolver {
	
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
		
		KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> aa =
				new KeyValueRecordPersister<>(relationRecordMapping, dialect, connectionConfiguration);
		
		
		IdAccessor<SRC, SRCID> srcIdAccessor = sourcePersister.getMapping();
		if (resolvedRelation.getValueEntity() != null) {
			KeyValueRecordPersister<X, VID, SRCID, MAPTABLE> relationRecordPersister = (KeyValueRecordPersister<X, VID, SRCID, MAPTABLE>) aa;
			
			
			Accessor<SRC, Collection<KeyValueRecord<X, VID, SRCID>>> collectionProviderForInsert =
					toRecordCollectionProvider(resolvedRelation, srcIdAccessor, false);
			// First, the target instances must be persisted to avoid foreign key errors
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, V>(valueEntityPersister) {
				@Override
				protected Collection<V> getTargets(SRC src) {
					List<V> keyEntities = nullable(resolvedRelation.getAccessor().get(src))
							.map(Map::values)
							// - we force type to List<K> to let the getOr() call returns any kind of List, else we are stuck with ArrayList
							// - we copy the result to a List to avoid the Map being tempered with due to the remove(null), because keySet() is backed by the Map
							.<List<V>>map(ArrayList::new)
							.getOr(Collections::emptyList);
					// Maps allow null value, so me remove them
					keyEntities.remove(null);
					return keyEntities;
				}
			});
			// Then, the Map entries can be persisted
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
			
			
			Accessor<SRC, Set<Entry<K, V>>> targetEntriesGetter = new Functions.NullProofFunction<>(resolvedRelation.getAccessor()::get).andThen(Map::entrySet)::apply;
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<X, VID, SRCID>> entryKeyValueRecordFunction =
					(record, srcId) -> (KeyValueRecord<X, VID, SRCID>) new KeyValueRecord<>(srcId, record.getKey(), valueEntityPersister.getId(record.getValue()));
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(targetEntriesGetter, valueEntityPersister,
					relationRecordPersister, sourcePersister, resolvedRelation.getValueEntityRelationMode(),
					Entry::getValue, entryKeyValueRecordFunction
			);
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
			
			if (resolvedRelation.getValueEntityRelationMode() != CascadeOptions.RelationMode.READ_ONLY) {
				Accessor<SRC, Collection<KeyValueRecord<X, VID, SRCID>>> recordsProviderAsPersistedInstances = toRecordCollectionProvider(
						resolvedRelation,
						srcIdAccessor,
						true);
				
				sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
			}
			
			if (resolvedRelation.getValueEntityRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL) {
				Function<SRC, Set<V>> targetEntitiesGetter = new Functions.NullProofFunction<>(resolvedRelation.getAccessor()::get).andThen(Map::entrySet).andThen(entries -> entries.stream().map(Entry::getValue).collect(Collectors.toSet()));
				sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, V>(valueEntityPersister) {
					@Override
					protected Collection<V> getTargets(SRC src) {
						return targetEntitiesGetter.apply(src);
					}
				});
			}
		}
		
		if (resolvedRelation.getKeyEntity() != null) {
			KeyValueRecordPersister<KID, Y, SRCID, MAPTABLE> relationRecordPersister = (KeyValueRecordPersister<KID, Y, SRCID, MAPTABLE>) aa;
			
			Accessor<SRC, Collection<KeyValueRecord<KID, Y, SRCID>>> collectionProviderForInsert =
					toRecordCollectionProvider2(resolvedRelation, srcIdAccessor, false);
			// First, the target instances must be persisted to avoid foreign key errors
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, K>(keyEntityPersister) {
				@Override
				protected Collection<K> getTargets(SRC src) {
					List<K> keyEntities = nullable(resolvedRelation.getAccessor().get(src))
							.map(Map::keySet)
							// - we force type to List<K> to let the getOr() call returns any kind of List, else we are stuck with ArrayList
							// - we copy the result to a List to avoid the Map being tempered with due to the remove(null), because keySet() is backed by the Map
							.<List<K>>map(ArrayList::new)
							.getOr(Collections::emptyList);
					// Some Map implementations allow null key, so me remove them
					keyEntities.remove(null);
					return keyEntities;
				}
			});
			// Then, the Map entries can be persisted
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
			
			Accessor<SRC, Set<Entry<K, V>>> targetEntriesGetter = new Functions.NullProofFunction<>(resolvedRelation.getAccessor()::get).andThen(Map::entrySet)::apply;
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KID, Y, SRCID>> entryKeyValueRecordFunction =
					(record, srcId) -> (KeyValueRecord<KID, Y, SRCID>) new KeyValueRecord<>(srcId, keyEntityPersister.getId(record.getKey()), record.getValue());
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(targetEntriesGetter, keyEntityPersister,
					relationRecordPersister, sourcePersister, resolvedRelation.getKeyEntityRelationMode(),
					Entry::getKey, entryKeyValueRecordFunction
			);
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
			
			if (resolvedRelation.getKeyEntityRelationMode() != CascadeOptions.RelationMode.READ_ONLY) {
				Accessor<SRC, Collection<KeyValueRecord<KID, Y, SRCID>>> recordsProviderAsPersistedInstances = toRecordCollectionProvider2(
						resolvedRelation,
						srcIdAccessor,
						true);
				
				sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
			}
			
			if (resolvedRelation.getKeyEntityRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL) {
				Function<SRC, Set<K>> targetEntitiesGetter = new Functions.NullProofFunction<>(resolvedRelation.getAccessor()::get).andThen(Map::entrySet).andThen(entries -> entries.stream().map(Entry::getKey).collect(Collectors.toSet()));
				sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, K>(keyEntityPersister) {
					@Override
					protected Collection<K> getTargets(SRC src) {
						return targetEntitiesGetter.apply(src);
					}
				});
			}
		}
		
		if (resolvedRelation.getKeyEntity() == null && resolvedRelation.getValueEntity() == null) {
			KeyValueRecordPersister<K, V, SRCID, MAPTABLE> relationRecordPersister = (KeyValueRecordPersister<K, V, SRCID, MAPTABLE>) aa;
			
			Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> collectionProviderForInsert =
					src -> Iterables.collect(
							nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
							entry -> new KeyValueRecord<>(srcIdAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(false),
							HashSet::new);
			// The Map entries can be persisted
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
			
			Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> collectionProviderAsPersistedInstances = src -> Iterables.collect(
					nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
					entry -> new KeyValueRecord<>(srcIdAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(true),
					HashSet::new);
			
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new CollectionUpdater<SRC, KeyValueRecord<K, V, SRCID>, Collection<KeyValueRecord<K, V, SRCID>>>(
					collectionProviderAsPersistedInstances,
					relationRecordPersister,
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
					relationRecordPersister.insert(updateContext.getAddedElements());
				}
			};
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
			
			if (resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY) {
				Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> recordsProviderAsPersistedInstances = src -> Iterables.collect(
						nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
						entry -> new KeyValueRecord<>(srcIdAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(true),
						HashSet::new);
				
				sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
			}
		}
		
		return aa;
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
		
		EntryMemberMapping<V, MAPTABLE> valueEntityIdentifierMapping = resolvedRelation.getValueEntityIdentifierMapping();
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
		
		
		if (resolvedRelation.getKeyEntityIdentifierMapping() instanceof ScalarMemberMapping) {
			Column<MAPTABLE, K> keyColumn = (Column<MAPTABLE, K>) ((ScalarMemberMapping<X, MAPTABLE>) resolvedRelation.getKeyEntityIdentifierMapping()).getColumn();
			KeyValueRecordIdMapping<K, SRCID, MAPTABLE> scalarMapping = new KeyValueRecordIdMapping<>(
					mapTable,
					columnedRow -> columnedRow.get(keyColumn),
					(Function<K, Map<Column<MAPTABLE, ?>, ?>>) k -> (Map) Maps.forHashMap(Column.class, Object.class).add(keyColumn, k),
					sourceIdentifierAssembler,
					resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
			// here, X is actually K because the entry key is a single value, not a composite one
			idMapping = (KeyValueRecordIdMapping<X, SRCID, MAPTABLE>) scalarMapping;
			
			propertiesMapping.put((ReadWritePropertyAccessPoint) KeyValueRecord.KEY_ACCESSOR, keyColumn);
		} else if (resolvedRelation.getKeyEntityIdentifierMapping() instanceof CompositeMemberMapping) {
			CompositeMemberMapping<KID, MAPTABLE> keyEntityIdentifierMapping = (CompositeMemberMapping<KID, MAPTABLE>) resolvedRelation.<KID>getKeyEntityIdentifierMapping();
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
			throw new IllegalArgumentException("Unsupported key entity identifier mapping : " + resolvedRelation.getKeyEntityIdentifierMapping());
		}
		return new KeyValueRecordMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				propertiesMapping,
				idMapping);
		
	}
	
	private static <X, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Accessor<SRC, Collection<KeyValueRecord<X, VID, SRCID>>> toRecordCollectionProvider(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, ?, ?> resolvedRelation,
	                                                                                    IdAccessor<SRC, SRCID> idAccessor,
	                                                                                    boolean markAsPersisted) {
		
		return src -> Iterables.collect(
				nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
				// for now we consider X as K
				// TODO: implement the composite case for key (both as embeddable and composite key of another entity)
				entry -> (KeyValueRecord<X, VID, SRCID>) new KeyValueRecord<>(idAccessor.getId(src), entry.getKey(), resolvedRelation.getValueEntity().getIdAccessor().get(entry.getValue())).setPersisted(markAsPersisted),
				HashSet::new);
	}
	
	private static <Y, SRC, SRCID, K, KID, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Accessor<SRC, Collection<KeyValueRecord<KID, Y, SRCID>>> toRecordCollectionProvider2(ResolvedMapRelation<SRC, SRCID, K, KID, V, ?, M, LEFTTABLE, MAPTABLE, ?, ?> resolvedRelation,
	                                                                                    IdAccessor<SRC, SRCID> idAccessor,
	                                                                                    boolean markAsPersisted) {
		
		return src -> Iterables.collect(
				nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
				entry -> (KeyValueRecord<KID, Y, SRCID>) new KeyValueRecord<>(idAccessor.getId(src), resolvedRelation.getKeyEntity().getIdAccessor().get(entry.getKey()), entry.getValue()).setPersisted(markAsPersisted),
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

