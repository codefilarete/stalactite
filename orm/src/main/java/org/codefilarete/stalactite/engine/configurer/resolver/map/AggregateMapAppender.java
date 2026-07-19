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
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.tool.Nullable.nullable;

public class AggregateMapAppender {
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	void append(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	            EntityJoinTree<SRC, SRCID> aggregateTree,
	            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	            String mountPoint,
	            KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
	            EntityReader<K, KID, KTABLE> keyEntityReader,
				EntityReader<V, VID, VTABLE> valueEntityReader) {
		
		ReadWritePropertyAccessPoint<SRC, M> mapAccessor = resolvedRelation.getAccessor();
		
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
				resolvedRelation,
				mapAccessor,
				inMemoryRelationHolder,
				keyValueRecordPersister,
				sourcePersister.getEntityJoinTree(),
				mountPoint);
		
		// Functions expected to provide the values to be put into the map of the source entity after the select.
		// They'll consume the direct content of the in memory relation holder that is filled during the select
		// which can be raw values or identifiers to entities
		BiFunction<SRCID, X, K> keyAdapter;
		BiFunction<SRCID, Y, V> valueAdapter;
		
		if (resolvedRelation.getKeyEntityDefinition() != null) {
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
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryKeyRelationHolder.clear();
				}
			});
			
			ForeignKey<MAPTABLE, KTABLE, KID> keyEntityReferenceMapping = resolvedRelation.getKeyEntityDefinition().getForeignKey();
			
			appendEntityJoin(aggregateTree, mapJoinNodeName, mapAccessor, keyEntityReader, keyEntityReferenceMapping, inMemoryKeyRelationHolder, record -> (KID) record.getKey());
		} else {
			// since there's no key entity, a simple cast is enough
			keyAdapter = (srcid, leftRawValue) -> (K) leftRawValue;
		}
		
		if (resolvedRelation.getValueEntityDefinition() != null) {
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
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryValueRelationHolder.clear();
				}
			});
			
			ForeignKey<MAPTABLE, VTABLE, VID> keyEntityReferenceMapping = resolvedRelation.getValueEntityDefinition().getForeignKey();
			
			appendEntityJoin(aggregateTree, mapJoinNodeName, mapAccessor, valueEntityReader, keyEntityReferenceMapping, inMemoryValueRelationHolder, record -> (VID) record.getValue());
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
		sourcePersister.addSelectListener(inMemoryRelationHolderInitializer.then(
				new SelectListener<SRC, SRCID>() {
					
					@Override
					public void afterSelect(Set<? extends SRC> result) {
						BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
								mapAccessor,
								mapAccessor,
								resolvedRelation.getComponentFactory(),
								(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
						result.forEach(bean -> {
							Collection<Duo<K, V>> keyValuePairs = finalInMemoryRelationAdapter.apply(sourcePersister.getMapping().getId(bean));
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
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
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
	void appendEntityJoin(EntityJoinTree<SRC, SRCID> aggregateTree,
	                      String mapJoinNodeName,
	                      ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                      EntityReader<ENTITY, ENTITY_ID, ENTITYTABLE> entityPersister,
	                      ForeignKey<MAPTABLE, ENTITYTABLE, ENTITY_ID> foreignKey,
	                      InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY> inMemoryRelationHolder,
						  Function<KeyValueRecord<?, ?, SRCID>, ENTITY_ID> entityIdExtractor) {
		
		aggregateTree.addRelationJoin(
				mapJoinNodeName,
				new EntityMappingAdapter<>(entityPersister.getMapping()),
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

