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
		
		KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> relationRecordPersister =
				new KeyValueRecordPersister<>(relationRecordMapping, dialect, connectionConfiguration);
		
		
		IdAccessor<SRC, SRCID> srcIdAccessor = sourcePersister.getMapping();
		
		// First, the target instances must be persisted to avoid foreign key errors.
		// These two blocks are independent so the "key & value are entities" case naturally registers both cascaders.
		if (resolvedRelation.getKeyEntity() != null) {
			registerTargetEntitiesInsertCascader(sourcePersister, keyEntityPersister, resolvedRelation.getAccessor(), Map::keySet);
		}
		if (resolvedRelation.getValueEntity() != null) {
			registerTargetEntitiesInsertCascader(sourcePersister, valueEntityPersister, resolvedRelation.getAccessor(), Map::values);
		}
		
		// Update cascade : a single association table row carries both key and value, hence its records must be managed by a
		// single updater. Therefore the "key & value are entities" case needs its dedicated updater (cascading to both
		// persisters at once), while the single-entity cases share registerSingleEntityMapUpdater(..) and the fully scalar
		// case relies on a plain CollectionUpdater. The conditions below are mutually exclusive, hence the separate ifs.
		if (resolvedRelation.getKeyEntity() != null && resolvedRelation.getValueEntity() != null) {
			
			KeyValueRecordPersister<KID, VID, SRCID, MAPTABLE> entityAsValueRecordPersister = (KeyValueRecordPersister<KID, VID, SRCID, MAPTABLE>) relationRecordPersister;
			
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KID, VID, SRCID>> entryKeyValueRecordFunction =
					(record, srcId) -> new KeyValueRecord<>(srcId, keyEntityPersister.getId(record.getKey()), valueEntityPersister.getId(record.getValue()));
			// When both key and value are entities, the update has to propagate to both persisters at once, hence a dedicated
			// EntityWriter is required (the single-entity helper cannot be reused here)
			CollectionUpdater.EntityWriter<Entry<K, V>, Integer> entityPersister = new EntityWriter<Entry<K, V>, Integer>() {
				
				@Override
				public Integer getId(Entry<K, V> entity) {
					return 31 * keyEntityPersister.getId(entity.getKey()).hashCode()
							+ valueEntityPersister.getId(entity.getValue()).hashCode();
				}
				
				@Override
				public void delete(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.delete(Iterables.<Entry<K, V>, K, Set<K>>collect(entities, Entry::getKey, HashSet::new));
					valueEntityPersister.delete(Iterables.<Entry<K, V>, V, Set<V>>collect(entities, Entry::getValue, HashSet::new));
				}
				
				@Override
				public void insert(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.insert(Iterables.<Entry<K, V>, K, Set<K>>collect(entities, Entry::getKey, HashSet::new));
					valueEntityPersister.insert(Iterables.<Entry<K, V>, V, Set<V>>collect(entities, Entry::getValue, HashSet::new));
				}
				
				@Override
				public void persist(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.persist(Iterables.<Entry<K, V>, K, Set<K>>collect(entities, Entry::getKey, HashSet::new));
					valueEntityPersister.persist(Iterables.<Entry<K, V>, V, Set<V>>collect(entities, Entry::getValue, HashSet::new));
				}
				
				@Override
				public void updateById(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.updateById(Iterables.<Entry<K, V>, K, Set<K>>collect(entities, Entry::getKey, HashSet::new));
					valueEntityPersister.updateById(Iterables.<Entry<K, V>, V, Set<V>>collect(entities, Entry::getValue, HashSet::new));
				}
				
				@Override
				public void update(Iterable<? extends Duo<Entry<K, V>, Entry<K, V>>> differencesIterable, boolean allColumnsStatement) {
					Set<Duo<K, K>> keys = Iterables.collect(differencesIterable, duo -> new Duo<>(duo.getLeft().getKey(), duo.getRight().getKey()), HashSet::new);
					keyEntityPersister.update(keys, allColumnsStatement);
					Set<Duo<V, V>> values = Iterables.collect(differencesIterable, duo -> new Duo<>(duo.getLeft().getValue(), duo.getRight().getValue()), HashSet::new);
					valueEntityPersister.update(values, allColumnsStatement);
				}
			};
			Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(entriesGetter(resolvedRelation.getAccessor()), entityPersister,
					entityAsValueRecordPersister, sourcePersister, resolvedRelation.getValueEntityRelationMode(),
					Function.identity(), entryKeyValueRecordFunction
			);
			sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
		}
		if (resolvedRelation.getKeyEntity() == null && resolvedRelation.getValueEntity() != null) {
			KeyValueRecordPersister<X, VID, SRCID, MAPTABLE> entityAsValueRecordPersister = (KeyValueRecordPersister<X, VID, SRCID, MAPTABLE>) relationRecordPersister;
			
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<X, VID, SRCID>> entryKeyValueRecordFunction =
					(record, srcId) -> (KeyValueRecord<X, VID, SRCID>) new KeyValueRecord<>(srcId, record.getKey(), valueEntityPersister.getId(record.getValue()));
			registerSingleEntityMapUpdater(sourcePersister, resolvedRelation.getAccessor(), valueEntityPersister,
					entityAsValueRecordPersister, resolvedRelation.getValueEntityRelationMode(), Entry::getValue, entryKeyValueRecordFunction);
		}
		if (resolvedRelation.getKeyEntity() != null && resolvedRelation.getValueEntity() == null) {
			KeyValueRecordPersister<KID, Y, SRCID, MAPTABLE> entityAsKeyRecordPersister = (KeyValueRecordPersister<KID, Y, SRCID, MAPTABLE>) relationRecordPersister;
			
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KID, Y, SRCID>> entryKeyValueRecordFunction =
					(record, srcId) -> (KeyValueRecord<KID, Y, SRCID>) new KeyValueRecord<>(srcId, keyEntityPersister.getId(record.getKey()), record.getValue());
			registerSingleEntityMapUpdater(sourcePersister, resolvedRelation.getAccessor(), keyEntityPersister,
					entityAsKeyRecordPersister, resolvedRelation.getKeyEntityRelationMode(), Entry::getKey, entryKeyValueRecordFunction);
		}
		if (resolvedRelation.getKeyEntity() == null && resolvedRelation.getValueEntity() == null) {
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
		
		// Orphan removal : independent per side so the "key & value are entities" case naturally registers both cascaders.
		if (resolvedRelation.getKeyEntity() != null) {
			registerOrphanRemovalDeleteCascader(sourcePersister, keyEntityPersister, resolvedRelation.getAccessor(), resolvedRelation.getKeyEntityRelationMode(), Entry::getKey);
		}
		if (resolvedRelation.getValueEntity() != null) {
			registerOrphanRemovalDeleteCascader(sourcePersister, valueEntityPersister, resolvedRelation.getAccessor(), resolvedRelation.getValueEntityRelationMode(), Entry::getValue);
		}
		
		if (resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY) {
			Function<K, X> keyAdapter;
			if (resolvedRelation.getKeyEntity() == null) {
				keyAdapter = k -> (X) k;
			} else {
				keyAdapter = k -> (X) resolvedRelation.getKeyEntity().getIdAccessor().get(k);
			}
			Function<V, Y> valueAdapter;
			if (resolvedRelation.getValueEntity() == null) {
				valueAdapter = v -> (Y) v;
			} else {
				valueAdapter = v -> (Y) resolvedRelation.getValueEntity().getIdAccessor().get(v);
			}
			
			// We persist Map entries
			Accessor<SRC, Collection<KeyValueRecord<X, Y, SRCID>>> collectionProviderForInsert = toRecordCollectionProvider(resolvedRelation, srcIdAccessor, keyAdapter, valueAdapter, false);
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
			
			Accessor<SRC, Collection<KeyValueRecord<X, Y, SRCID>>> recordsProviderAsPersistedInstances = toRecordCollectionProvider(resolvedRelation, srcIdAccessor, keyAdapter, valueAdapter, true);
			sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
		}
		
		return relationRecordPersister;
	}
	
	/**
	 * Builds the accessor giving the {@link Entry} {@link Set} of the relation {@link Map} (null-safe).
	 */
	private static <SRC, K, V, M extends Map<K, V>> Accessor<SRC, Set<Entry<K, V>>> entriesGetter(Accessor<SRC, M> mapAccessor) {
		return new NullProofFunction<>(mapAccessor::get).andThen(Map::entrySet)::apply;
	}
	
	/**
	 * Registers an insert cascade that persists the target entities (keys or values) of the relation {@link Map} before the
	 * source is inserted, in order to avoid foreign key violations.
	 *
	 * @param targetsExtractor extracts the entities to persist from the {@link Map} (typically {@code Map::keySet} or {@code Map::values})
	 * @param <T> target entity type (either the key type or the value type)
	 */
	private static <SRC, K, V, T, M extends Map<K, V>> void registerTargetEntitiesInsertCascader(
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
	
	/**
	 * Registers the update cascade for a relation {@link Map} owning a single entity side (either key or value, the other side being a
	 * simple bean). The opposite side is stored as a raw value.
	 *
	 * @param entityPersister persister of the entity side
	 * @param entryBeanExtractor extracts the entity from a {@link Entry} (typically {@code Entry::getKey} or {@code Entry::getValue})
	 * @param recordBuilder builds the {@link KeyValueRecord} to be persisted from a {@link Entry} and the source identifier
	 * @param <ENTITY> entity side type (K or V)
	 * @param <KK> {@link KeyValueRecord} key type (key identifier or raw key)
	 * @param <VV> {@link KeyValueRecord} value type (value identifier or raw value)
	 */
	private static <SRC, SRCID, K, V, ENTITY, KK, VV, M extends Map<K, V>> void registerSingleEntityMapUpdater(
			ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
			Accessor<SRC, M> mapAccessor,
			ConfiguredRelationalPersister<ENTITY, ?> entityPersister,
			EntityPersister<KeyValueRecord<KK, VV, SRCID>, RecordId<KK, SRCID>> keyValueRecordPersister,
			CascadeOptions.RelationMode maintenanceMode,
			Function<? super Entry<K, V>, ENTITY> entryBeanExtractor,
			BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KK, VV, SRCID>> recordBuilder) {
		Mutator<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(entriesGetter(mapAccessor), entityPersister,
				keyValueRecordPersister, sourcePersister, maintenanceMode, entryBeanExtractor, recordBuilder);
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
	}
	
	/**
	 * Registers, when the given relation mode is {@link CascadeOptions.RelationMode#ALL_ORPHAN_REMOVAL}, a delete cascade that removes
	 * the target entities (keys or values) of the relation {@link Map} before the source is deleted.
	 *
	 * @param entryExtractor extracts the entity from a {@link Entry} (typically {@code Entry::getKey} or {@code Entry::getValue})
	 * @param <T> target entity type (either the key type or the value type)
	 */
	private static <SRC, K, V, T, M extends Map<K, V>> void registerOrphanRemovalDeleteCascader(
			ConfiguredRelationalPersister<SRC, ?> sourcePersister,
			ConfiguredRelationalPersister<T, ?> targetPersister,
			Accessor<SRC, M> mapAccessor,
			CascadeOptions.RelationMode relationMode,
			Function<? super Entry<K, V>, T> entryExtractor) {
		if (relationMode == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL) {
			Function<SRC, Set<T>> targetEntitiesGetter = new NullProofFunction<>(mapAccessor::get)
					.andThen(Map::entrySet)
					.andThen(entries -> entries.stream().map(entryExtractor).collect(Collectors.toSet()));
			sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, T>(targetPersister) {
				@Override
				protected Collection<T> getTargets(SRC src) {
					return targetEntitiesGetter.apply(src);
				}
			});
		}
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

