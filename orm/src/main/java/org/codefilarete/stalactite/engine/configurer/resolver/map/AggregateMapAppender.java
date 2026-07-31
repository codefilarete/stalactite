package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.MapMemberAsEntity;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;
import static org.codefilarete.tool.Nullable.nullable;

public class AggregateMapAppender {
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> append(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> relation,
	                                                                                       EntityJoinTree<SRC, SRCID> aggregateTree,
	                                                                                       EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                                                       String mountPoint,
	                                                                                       KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
	                                                                                       EntityReader<K, KID, KTABLE> keyEntityReader,
	                                                                                       EntityReader<V, VID, VTABLE> valueEntityReader,
	                                                                                       Dialect dialect,
	                                                                                       ConnectionProvider connectionProvider) {
		
		Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> result;
		
		if (relation.isFetchSeparately()) {
			
			DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join = relation.getJoin();
			
			// adding second phase loader
			KeyMapping<LEFTTABLE, MAPTABLE, SRCID> targetPkToRightKey = new KeyMapping<>(sourcePersister.getMapping().getTargetTable().getPrimaryKey(), join.getRightKey());
			KeepOrderMap<JoinLink<LEFTTABLE, ?>, JoinLink<MAPTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
			
			MapEntryLoader<SRC, SRCID, X, Y, LEFTTABLE, MAPTABLE> mapEntryLoader = new MapEntryLoader<>(
					sourcePersister.getMapping().getIdMapping(),
					keyValueRecordPersister,
					targetPkToAssociationTableKey,
					dialect,
					connectionProvider);
			
			result = new Duo<>();
			
			SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID> mapEntrySelectExecutor = mapEntryLoader;
			if (relation.getKeyEntityDefinition() != null) {
				Duo<SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID>, GraftPoint> keyJoinResult = appendSeparatelyFetchedEntityJoin(
						mapEntryLoader,
						relation.getAccessor(),
						keyEntityReader,
						relation.getKeyEntityDefinition(),
						mapEntrySelectExecutor,
						KeyValueRecord::getKey,
						KeyValueRecord::setKey);
				mapEntrySelectExecutor = keyJoinResult.getLeft();
				result.setLeft(keyJoinResult.getRight());
			}
			
			if (relation.getValueEntityDefinition() != null) {
				// Note that because the relation is loaded separately, next joins should be appended to the second-phase entity join tree,
				// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
				Duo<SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID>, GraftPoint> valueJoinResult = appendSeparatelyFetchedEntityJoin(
						mapEntryLoader,
						relation.getAccessor(),
						valueEntityReader,
						relation.getValueEntityDefinition(),
						mapEntrySelectExecutor,
						KeyValueRecord::getValue,
						KeyValueRecord::setValue);
				mapEntrySelectExecutor = valueJoinResult.getLeft();
				result.setRight(valueJoinResult.getRight());
			}
						
			SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID> eventuallyRearrangingMapEntrySelectExecutor = mapEntrySelectExecutor;
			// Adding a listener that loads the entries after the main entities
			// Note that the selector may be a wrapper that combine the initial raw results (entities identifiers) with the real entities kept in memory
			ReadWritePropertyAccessPoint<SRC, M> mapAccessor = relation.getAccessor();
			BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
					mapAccessor,
					mapAccessor,
					relation.getComponentFactory(),
					(bean, duo, map) -> map.put(duo.getKey(), duo.getValue()));
			Function<Collection<KeyValueRecord<X, Y, SRCID>>, Collection<KeyValueRecord<X, Y, SRCID>>> finalInMemoryRelationAdapterWithOrdering = entries -> {
				if (relation.isOrdered()) {
					return entries.stream()
							.sorted(Comparator.comparingInt(KeyValueRecord::getIndex))
							.collect(Collectors.toList());
				} else {
					return entries;
				}
			};
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Set<SRCID> srcIds = Iterables.collect(result, sourcePersister.getMapping()::getId, HashSet::new);
					Set<KeyValueRecord<X, Y, SRCID>> select = eventuallyRearrangingMapEntrySelectExecutor.select(srcIds);
					
					// we sew the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						LinkedHashSet<KeyValueRecord<X, Y, SRCID>> collect = select.stream()
								.filter(record -> record.getId().getId().equals(sourcePersister.getMapping().getId(src)))
								.collect(Collectors.toCollection(LinkedHashSet::new));
						finalInMemoryRelationAdapterWithOrdering.apply(collect)
								.forEach(entry -> originalRelationFixer.apply(src, (KeyValueRecord<K, V, SRCID>) entry));
					});
				}
			});
			return result;
		} else {
			result = append2(relation, aggregateTree, sourcePersister, mountPoint, keyValueRecordPersister, keyEntityReader, valueEntityReader);
		}
		return result;
	}
	
	/**
	 * Factorizes the logic shared by key and value entities when a map relation is fetched separately: adds a join
	 * on the {@link MapEntryLoader} entity join tree to collect the entity in-memory, then wraps the given select
	 * executor so it rearranges its result by replacing the raw identifier with the found entity.
	 */
	private <X, Y, SRC, SRCID, ENTITY_ID, ENTITY, RAW, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, ENTITYTABLE extends Table<ENTITYTABLE>>
	Duo<SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID>, GraftPoint> appendSeparatelyFetchedEntityJoin(
			MapEntryLoader<SRC, SRCID, X, Y, LEFTTABLE, MAPTABLE> mapEntryLoader,
			ReadWritePropertyAccessPoint<SRC, ?> mapAccessor,
			EntityReader<ENTITY, ENTITY_ID, ENTITYTABLE> entityReader,
			MapMemberAsEntity<ENTITY, ENTITY_ID, MAPTABLE, ENTITYTABLE, ?> entityDefinition,
			SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID> currentSelectExecutor,
			Function<KeyValueRecord<X, Y, SRCID>, RAW> rawValueGetter,
			BiConsumer<KeyValueRecord<X, Y, SRCID>, RAW> rawValueSetter) {
		
		InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY> inMemoryEntityHolder = new InMemoryRelationHolder<>();
		
		// We add a join on the MapEntryLoader to collect the entity in-memory, then we rearrange the result
		String entityJoinNodeName = appendEntityJoin(
				(EntityJoinTree<SRC, SRCID>) mapEntryLoader.getEntityJoinTree(),
				ROOT_JOIN_NAME,
				(ReadWritePropertyAccessPoint) mapAccessor,
				entityReader,
				entityDefinition.getForeignKey(),
				inMemoryEntityHolder,
				record -> (ENTITY_ID) rawValueGetter.apply((KeyValueRecord<X, Y, SRCID>) record));
		
		// we wrap the entry loader by some code that rearranges its result by getting the entities from the in-memory relation holder
		SelectExecutor<KeyValueRecord<X, Y, SRCID>, SRCID> wrappedSelectExecutor = ids -> {
			try {
				inMemoryEntityHolder.init();
				
				Set<KeyValueRecord<X, Y, SRCID>> select = currentSelectExecutor.select(ids);
				
				// rearranging the previous result which contains only the raw identifier by replacing it with the final entity
				// tempering with this allows not to change the final relation sewing
				select.forEach(record -> {
					Collection<MapEntry<ENTITY_ID, ENTITY>> duos = inMemoryEntityHolder.giveEntityEntries(record.getId().getId());
					duos.forEach(duo -> {
						if (duo.getLeft().equals(rawValueGetter.apply(record))) {
							rawValueSetter.accept(record, (RAW) duo.getRight());
						}
					});
				});
				return select;
			} finally {
				// we remove the internal ThreadLocal
				inMemoryEntityHolder.clear();
			}
		};
		
		return new Duo<>(wrappedSelectExecutor, new GraftPoint(entityDefinition.getEntity(), entityReader, entityJoinNodeName));
	}
	
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> append2(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> relation,
	                                                                                        EntityJoinTree<SRC, SRCID> aggregateTree,
	                                                                                        EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                                                        String mountPoint,
	                                                                                        KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
	                                                                                        EntityReader<K, KID, KTABLE> keyEntityReader,
	                                                                                        EntityReader<V, VID, VTABLE> valueEntityReader) {
		
		Duo<GraftPoint, GraftPoint> result = new Duo<>();
		
		ReadWritePropertyAccessPoint<SRC, M> mapAccessor = relation.getAccessor();
		
		InMemoryRelationHolder<SRCID, X, Y> inMemoryRelationHolder = new InMemoryRelationHolder<>();
		
		SelectListener<SRC, SRCID> inMemoryRelationHolderInitializer = new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
		};
		
		SelectListener<SRC, SRCID> inMemoryRelationHolderClearer = new SelectListener<SRC, SRCID>() {
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				inMemoryRelationHolder.clear();
			}
		};
		
		String mapJoinNodeName = appendAssociationTableJoin(
				relation,
				mapAccessor,
				inMemoryRelationHolder,
				keyValueRecordPersister,
				aggregateTree,
				mountPoint);
		
		// Functions expected to provide the values to be put into the map of the source entity after the select.
		// They'll consume the direct content of the in memory relation holder that is filled during the select
		// which can be raw values or identifiers to entities
		BiFunction<SRCID, X, K> keyAdapter;
		BiFunction<SRCID, Y, V> valueAdapter;
		
		MapMemberAsEntity<K, KID, MAPTABLE, KTABLE, ?> keyEntityDefinition = relation.getKeyEntityDefinition();
		if (keyEntityDefinition != null) {
			// we keep the link between id and entity found through the join and then use it to build the final map
			InMemoryRelationHolder<SRCID, KID, K> inMemoryKeyRelationHolder = new InMemoryRelationHolder<>();
			
			// the final map is made of the entities found in the in-memory relation holder
			keyAdapter = (srcid, leftRawValue) ->
				nullable(Iterables.find(inMemoryKeyRelationHolder.giveEntityEntries(srcid), duo -> duo.getLeft().equals(leftRawValue))).map(MapEntry::getRight).get();
			// we ask for our own relation holder to be initialized and cleared
			inMemoryRelationHolderInitializer = inMemoryRelationHolderInitializer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryKeyRelationHolder.init();
				}
			});	
			inMemoryRelationHolderClearer = inMemoryRelationHolderClearer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryKeyRelationHolder.clear();
				}
			});
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryKeyRelationHolder.clear();
				}
			});
			
			String keyEntityJoinNodeName = appendEntityJoin(aggregateTree, mapJoinNodeName, mapAccessor, keyEntityReader, keyEntityDefinition.getForeignKey(), inMemoryKeyRelationHolder, record -> (KID) record.getKey());
			result.setLeft(new GraftPoint(keyEntityDefinition.getEntity(), keyEntityReader, keyEntityJoinNodeName));
		} else {
			// since there's no key entity, a simple cast is enough
			keyAdapter = (srcid, leftRawValue) -> (K) leftRawValue;
		}
		
		MapMemberAsEntity<V, VID, MAPTABLE, VTABLE, ?> valueEntityDefinition = relation.getValueEntityDefinition();
		if (valueEntityDefinition != null) {
			// we keep the link between id and entity found through the join and then use it to build the final map
			InMemoryRelationHolder<SRCID, VID, V> inMemoryValueRelationHolder = new InMemoryRelationHolder<>();
			
			// the final map is made of the entities found in the in-memory relation holder
			valueAdapter = (srcid, rightRawValue) ->
				nullable(Iterables.find(inMemoryValueRelationHolder.giveEntityEntries(srcid), duo -> duo.getLeft().equals(rightRawValue))).map(MapEntry::getRight).get();
			// we ask for our own relation holder to be initialized and cleared
			inMemoryRelationHolderInitializer = inMemoryRelationHolderInitializer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryValueRelationHolder.init();
				}
			});
			inMemoryRelationHolderClearer = inMemoryRelationHolderClearer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryValueRelationHolder.clear();
				}
			});
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryValueRelationHolder.clear();
				}
			});
			
			ForeignKey<MAPTABLE, VTABLE, VID> keyEntityReferenceMapping = valueEntityDefinition.getForeignKey();
			
			String valueEntityJoinNodeName = appendEntityJoin(aggregateTree, mapJoinNodeName, mapAccessor, valueEntityReader, keyEntityReferenceMapping, inMemoryValueRelationHolder, record -> (VID) record.getValue());
			result.setLeft(new GraftPoint(valueEntityDefinition.getEntity(), valueEntityReader, valueEntityJoinNodeName));
		} else {
			// since there's no value entity, a simple cast is enough
			valueAdapter = (srcid, rightRawValue) -> (V) rightRawValue;
		}
		
		Function<SRCID, Set<MapEntry<K, V>>> finalInMemoryRelationAdapter = srcid -> {
			Collection<MapEntry<X, Y>> duos = inMemoryRelationHolder.giveEntityEntries(srcid);
			if (duos != null) {
				return Iterables.collect(duos, duo -> {
					// We change Duo values by replacing them by the adapters value
					// Note that we could have recreated a new IndexedDuo instance with those values, it would have been
					// clearer, but it's kind of superflous and would consume some more memory, and also may require
					// some more code maintenance if we change MemoryHolder internal storage type : the right one must
					// be instanciated again here.
					((MapEntry<K, V>) duo).setLeft(keyAdapter.apply(srcid, duo.getLeft()));
					((MapEntry<K, V>) duo).setRight(valueAdapter.apply(srcid, duo.getRight()));
					return (MapEntry<K, V>) duo;
				}, HashSet::new);
			} else {
				return null;
			}
		};
		BeanRelationFixer<SRC, MapEntry<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				relation.getComponentFactory(),
				(bean, entry, map) -> map.put(entry.getLeft(), entry.getRight()));
		Function<Collection<MapEntry<K, V>>, Collection<MapEntry<K, V>>> finalInMemoryRelationAdapterWithOrdering = entries -> {
			if (relation.isOrdered()) {
				return entries.stream()
						.sorted(Comparator.comparingInt(MapEntry::getIndex))
						.collect(Collectors.toList());
			} else {
				return entries;
			}
		};
		sourcePersister.addSelectListener(inMemoryRelationHolderInitializer.then(
				new SelectListener<SRC, SRCID>() {
					
					@Override
					public void afterSelect(Set<? extends SRC> result) {
						result.forEach(bean -> {
							Collection<MapEntry<K, V>> keyValuePairs = finalInMemoryRelationAdapter.apply(sourcePersister.getMapping().getId(bean));
							
							if (keyValuePairs != null) {
								keyValuePairs = finalInMemoryRelationAdapterWithOrdering.apply(keyValuePairs);
								keyValuePairs.forEach(entry -> originalRelationFixer.apply(bean, entry));
							} // else : no association record
						});
					}
				}).then(inMemoryRelationHolderClearer));
		
		return result;
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	String appendAssociationTableJoin(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                  ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                                  InMemoryRelationHolder<SRCID, X, Y> inMemoryRelationHolder,
	                                  KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
									  EntityJoinTree<SRC, SRCID> aggregateTree,
									  String mountPoint) {
		BeanRelationFixer<SRC, KeyValueRecord<X, Y, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				resolvedRelation.getComponentFactory(),
				(bean, record, map) -> {
					inMemoryRelationHolder.storeRelation(record.getId().getId(), record.getKey(), record.getValue(), record.getIndex());
				});
		
		return aggregateTree.addRelationJoin(
				mountPoint,
				new EntityMappingAdapter<>(keyValueRecordPersister.getMapping()),
				mapAccessor,
				resolvedRelation.getJoin().getLeftKey(),
				resolvedRelation.getJoin().getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
	}
	
	private <SRC, SRCID, K, V, ENTITY, ENTITY_ID, M extends Map<K, V>, MAPTABLE extends Table<MAPTABLE>, ENTITYTABLE extends Table<ENTITYTABLE>>
	String appendEntityJoin(EntityJoinTree<SRC, SRCID> aggregateTree,
	                      String mapJoinNodeName,
	                      ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                      EntityReader<ENTITY, ENTITY_ID, ENTITYTABLE> entityPersister,
	                      ForeignKey<MAPTABLE, ENTITYTABLE, ENTITY_ID> foreignKey,
	                      InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY> inMemoryRelationHolder,
						  Function<KeyValueRecord<?, ?, SRCID>, ENTITY_ID> entityIdExtractor) {
		
		return aggregateTree.addRelationJoin(
				mapJoinNodeName,
				new EntityMappingAdapter<>(entityPersister.getMapping()),
				mapAccessor,
				foreignKey.getSourceKey(),
				foreignKey.getReferencedKey(),
				null,
				OUTER,
				(bean, entity) -> {
					// because we joined with the map association table and KeyValueRecordPersister, we know that the given object is a KeyValueRecord
					KeyValueRecord<?, ?, SRCID> record = (KeyValueRecord<?, ?, SRCID>) bean;
					// We only store the link between source entity and related entity, here we don't care about the index
					// since that's not the goal of the current logic, and, anyway not possible. Index is the responsibility
					// of the association table join.
					inMemoryRelationHolder.storeRelation(record.getId().getId(), entityIdExtractor.apply(record), entity);
				},
				Collections.emptySet(),
				null);
	}
	
	static class InMemoryRelationHolder<I, K, V> {
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Set<MapEntry<K, V>>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		void storeRelation(I source, K key, V value) {
			Set<MapEntry<K, V>> relatedDuos = giveRelatedDuos(source);
			MapEntry<K, V> result = new MapEntry<>(key, value);
			relatedDuos.add(result);
		}
		
		void storeRelation(I source, K key, V value, Integer index) {
			Set<MapEntry<K, V>> relatedDuos = giveRelatedDuos(source);
			MapEntry<K, V> result = new MapEntry<>(key, value, index);
			relatedDuos.add(result);
		}
		
		private Set<MapEntry<K, V>> giveRelatedDuos(I source) {
			Map<I, Set<MapEntry<K, V>>> srcidcMap = relationCollectionPerEntity.get();
			return srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
		}
		
		Collection<MapEntry<K, V>> giveEntityEntries(I src) {
			Map<I, Set<MapEntry<K, V>>> currentMap = relationCollectionPerEntity.get();
			return nullable(currentMap)
					.map(map -> map.get(src))
					.get();
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
		}
	}
	
	private static class MapEntry<K, V> {
		
		private K left;
		private V right;
		private Integer index;
		
		public MapEntry(K left, V right) {
			this.left = left;
			this.right = right;
		}
		
		public MapEntry(K left, V right, Integer index) {
			this(left, right);
			this.index = index;
		}
		
		public void setLeft(K left) {
			this.left = left;
		}
		
		public K getLeft() {
			return left;
		}
		
		public void setRight(V right) {
			this.right = right;
		}
		
		public V getRight() {
			return right;
		}
		
		public Integer getIndex() {
			return index;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			MapEntry<?, ?> mapEntry = (MapEntry<?, ?>) o;
			return Objects.equals(left, mapEntry.left);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(left);
		}
	}
}

