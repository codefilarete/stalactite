package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.tool.Nullable.nullable;

public class AggregateMapAppender {
	
	private final MapResolver mapResolver;
	
	public AggregateMapAppender(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mapResolver = new MapResolver(dialect, connectionConfiguration, skeletonAggregateResolver);
	}
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	void append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	            ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	            AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn) {
		
		ReadWritePropertyAccessPoint<SRC, M> mapAccessor = resolvedRelation.getAccessor();
		
		Holder<ConfiguredRelationalPersister<K, KID>> keyEntityPersisterHolder = new Holder<>();
		Holder<ConfiguredRelationalPersister<V, VID>> valueEntityPersisterHolder = new Holder<>();
		
		KeyValueRecordPersister<?, ?, SRCID, MAPTABLE> keyValueRecordPersister = mapResolver.resolve(
				resolvedRelation,
				assemblyPawn.getRelationOwnerPersister(),
				keyEntityPersisterHolder::set,
				valueEntityPersisterHolder::set);
		
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
				rootPersister,
				resolvedRelation,
				assemblyPawn,
				mapAccessor,
				inMemoryRelationHolder,
				(KeyValueRecordPersister<X, Y, SRCID, MAPTABLE>) keyValueRecordPersister);
		
		// Functions expected to provide the values to be put into the map of the source entity after the select.
		// They'll consume the direct content of the in memory relation holder that is filled during the select
		// which can be raw values or identifiers to entities
		BiFunction<SRCID, X, K> keyAdapter;
		BiFunction<SRCID, Y, V> valueAdapter;
		
		if (resolvedRelation.getKeyEntity() != null) {
			// we keep the link between id and entity found through the join and then use it to build the final map
			InMemoryRelationHolder<SRCID, KID, K> inMemoryKeyRelationHolder = new InMemoryRelationHolder<>();
			
			// the final map is made of the entities found in the in-memory relation holder
			keyAdapter = (srcid, leftRawValue) -> {
				Map<KID, K> entities = Iterables.map(inMemoryKeyRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
				return entities.get((K) leftRawValue);
			};
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
			rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryKeyRelationHolder.clear();
				}
			});
			
			ConfiguredRelationalPersister<K, KID> keyEntityPersister = keyEntityPersisterHolder.get();
			ForeignKey<MAPTABLE, KTABLE, KID> keyEntityReferenceMapping = resolvedRelation.getKeyEntityForeignKey();
			
			appendEntityJoin(rootPersister, mapJoinNodeName, mapAccessor, keyEntityPersister, keyEntityReferenceMapping, inMemoryKeyRelationHolder, record -> (KID) record.getKey());
		} else {
			// since there's no key entity, a simple cast is enough
			keyAdapter = (srcid, leftRawValue) -> (K) leftRawValue;
		}
		
		if (resolvedRelation.getValueEntity() != null) {
			// we keep the link between id and entity found through the join and then use it to build the final map
			InMemoryRelationHolder<SRCID, VID, V> inMemoryValueRelationHolder = new InMemoryRelationHolder<>();
			
			// the final map is made of the entities found in the in-memory relation holder
			valueAdapter = (srcid, rightRawValue) -> {
				Map<VID, V> entities = Iterables.map(inMemoryValueRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
				return entities.get((V) rightRawValue);
			};
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
			rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryValueRelationHolder.clear();
				}
			});
			
			ConfiguredRelationalPersister<V, VID> valueEntityPersister = valueEntityPersisterHolder.get();
			ForeignKey<MAPTABLE, VTABLE, VID> keyEntityReferenceMapping = resolvedRelation.getValueEntityForeignKey();
			
			appendEntityJoin(rootPersister, mapJoinNodeName, mapAccessor, valueEntityPersister, keyEntityReferenceMapping, inMemoryValueRelationHolder, record -> (VID) record.getValue());
		} else {
			// since there's no value entity, a simple cast is enough
			valueAdapter = (srcid, rightRawValue) -> (V) rightRawValue;
		}
		
		Function<SRCID, Set<Duo<K, V>>> finalInMemoryRelationAdapter = srcid -> {
			Collection<Duo<X, Y>> duos = inMemoryRelationHolder.get(srcid);
			if (duos != null) {
				return duos.stream().map(duo -> {
					return new Duo<>(keyAdapter.apply(srcid, duo.getLeft()), valueAdapter.apply(srcid, duo.getRight()));
				}).collect(Collectors.toSet());
			} else {
				return null;
			}
		};
		rootPersister.addSelectListener(inMemoryRelationHolderInitializer.then(
				new SelectListener<SRC, SRCID>() {
					
					@Override
					public void afterSelect(Set<? extends SRC> result) {
						BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
								mapAccessor,
								mapAccessor,
								resolvedRelation.getComponentFactory(),
								(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
						result.forEach(bean -> {
							Collection<Duo<K, V>> keyValuePairs = finalInMemoryRelationAdapter.apply(rootPersister.getId(bean));
							if (keyValuePairs != null) {
								keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
							} // else : no association record
						});
					}
				}).then(inMemoryRelationHolderClearer));
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	String appendAssociationTableJoin(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                                ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                                InMemoryRelationHolder<SRCID, X, Y> inMemoryRelationHolder,
	                                KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister) {
		BeanRelationFixer<SRC, KeyValueRecord<X, Y, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				resolvedRelation.getComponentFactory(),
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
				});
		
		return rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
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
	void appendEntityJoin(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                      String mapJoinNodeName,
	                      ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                      ConfiguredRelationalPersister<ENTITY, ENTITY_ID> entityPersister,
	                      ForeignKey<MAPTABLE, ENTITYTABLE, ENTITY_ID> foreignKey,
	                      InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY> inMemoryRelationHolder,
						  Function<KeyValueRecord<?, ?, SRCID>, ENTITY_ID> entityIdExtractor) {
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				mapJoinNodeName,
				new EntityMappingAdapter<>(entityPersister.<ENTITYTABLE>getMapping()),
				mapAccessor,
				foreignKey.getSourceKey(),
				foreignKey.getReferencedKey(),
				null,
				OUTER,
				(bean, input) -> {
					// because we joined with the map association table and KeyValueRecordPersister, we know that the given object is a KeyValueRecord
					KeyValueRecord<?, ?, SRCID> record = (KeyValueRecord<?, ?, SRCID>) bean;
					inMemoryRelationHolder.storeRelation(record.getId().getId(), entityIdExtractor.apply(record), input);
				},
				Collections.emptySet(),
				null);
	}
	
	public static class InMemoryRelationHolder<I, K, V> {
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Set<Duo<K, V>>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		
		public void storeRelation(I source, K key, V value) {
			Map<I, Set<Duo<K, V>>> srcidcMap = relationCollectionPerEntity.get();
			Set<Duo<K, V>> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
			Duo<K, V> duo = relatedDuos.stream().filter(pawn -> Objects.equals(pawn.getLeft(), key)).findAny().orElseGet(() -> {
				Duo<K, V> result = new Duo<>();
				relatedDuos.add(result);
				return result;
			});
			duo.setLeft(key);
			duo.setRight(value);
		}
		
		public Collection<Duo<K, V>> get(I src) {
			Map<I, Set<Duo<K, V>>> currentMap = relationCollectionPerEntity.get();
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
}

