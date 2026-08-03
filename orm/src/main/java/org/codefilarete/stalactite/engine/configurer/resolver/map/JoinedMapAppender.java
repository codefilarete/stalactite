package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.MapMemberAsEntity;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * Wires a {@link Map} relation with some direct joins onto an aggregate's {@link EntityJoinTree}:
 * the association table, and optionally the key and/or value entities, are joined directly onto it.
 * Their rows are collected in-memory ({@link InMemoryRelationHolder}) during the main select, so that the
 * resulting {@link Map} can be rebuilt and set onto the owning entity right after selection.
 * 
 * @author Guillaume Mary
 * @see FetchSeparatelyMapAppender
 */
public class JoinedMapAppender {
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> append(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> relation,
	                                                                                       EntityJoinTree<SRC, SRCID> aggregateTree,
	                                                                                       ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                                                       String mountPoint,
	                                                                                       KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister,
	                                                                                       ConfiguredEntityReader<K, KID, KTABLE> keyEntityReader,
	                                                                                       ConfiguredEntityReader<V, VID, VTABLE> valueEntityReader) {
		
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
			EntityRelationAdapter<SRC, SRCID, X, K> keyRelation = appendJoinedEntityRelation(
					aggregateTree,
					mapJoinNodeName,
					mapAccessor,
					keyEntityReader,
					keyEntityDefinition,
					sourcePersister,
					record -> (KID) record.getKey());
			keyAdapter = keyRelation.adapter;
			inMemoryRelationHolderInitializer = inMemoryRelationHolderInitializer.then(keyRelation.initializer);
			inMemoryRelationHolderClearer = inMemoryRelationHolderClearer.then(keyRelation.clearer);
			result.setLeft(keyRelation.graftPoint);
		} else {
			// since there's no key entity, a simple cast is enough
			keyAdapter = (srcid, leftRawValue) -> (K) leftRawValue;
		}
		
		MapMemberAsEntity<V, VID, MAPTABLE, VTABLE, ?> valueEntityDefinition = relation.getValueEntityDefinition();
		if (valueEntityDefinition != null) {
			EntityRelationAdapter<SRC, SRCID, Y, V> valueRelation = appendJoinedEntityRelation(
					aggregateTree,
					mapJoinNodeName,
					mapAccessor,
					valueEntityReader,
					valueEntityDefinition,
					sourcePersister,
					record -> (VID) record.getValue());
			valueAdapter = valueRelation.adapter;
			inMemoryRelationHolderInitializer = inMemoryRelationHolderInitializer.then(valueRelation.initializer);
			inMemoryRelationHolderClearer = inMemoryRelationHolderClearer.then(valueRelation.clearer);
			result.setRight(valueRelation.graftPoint); // fixed: was setLeft, overwriting the key's GraftPoint
		} else {
			// since there's no value entity, a simple cast is enough
			valueAdapter = (srcid, rightRawValue) -> (V) rightRawValue;
		}
		
		Function<SRCID, Set<InMemoryRelationHolder<SRCID, K, V>.MapEntry>> finalInMemoryRelationAdapter = srcid -> {
			Collection<InMemoryRelationHolder<SRCID, X, Y>.MapEntry> duos = inMemoryRelationHolder.giveEntityEntries(srcid);
			if (duos != null) {
				return Iterables.collect(duos, duo -> {
					// We change Duo values by replacing them by the adapters value
					// Note that we could have recreated a new IndexedDuo instance with those values, it would have been
					// clearer, but it's kind of superflous and would consume some more memory, and also may require
					// some more code maintenance if we change MemoryHolder internal storage type : the right one must
					// be instanciated again here.
					((InMemoryRelationHolder<SRCID, K, V>.MapEntry) duo).setLeft(keyAdapter.apply(srcid, duo.getLeft()));
					((InMemoryRelationHolder<SRCID, K, V>.MapEntry) duo).setRight(valueAdapter.apply(srcid, duo.getRight()));
					return (InMemoryRelationHolder<SRCID, K, V>.MapEntry) duo;	
				}, HashSet::new);
			} else {
				return null;
			}
		};
		BeanRelationFixer<SRC, InMemoryRelationHolder<SRCID, K, V>.MapEntry> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				relation.getComponentFactory(),
				(bean, entry, map) -> map.put(entry.getLeft(), entry.getRight()));
		Function<Collection<InMemoryRelationHolder<SRCID, K, V>.MapEntry>, Collection<InMemoryRelationHolder<SRCID, K, V>.MapEntry>> finalInMemoryRelationAdapterWithOrdering = entries -> {
			if (relation.isOrdered()) {
				return entries.stream()
						.sorted(Comparator.comparingInt(InMemoryRelationHolder.MapEntry::getIndex))
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
							Collection<InMemoryRelationHolder<SRCID, K, V>.MapEntry> keyValuePairs = finalInMemoryRelationAdapter.apply(sourcePersister.getMapping().getId(bean));
							
							if (keyValuePairs != null) {
								keyValuePairs = finalInMemoryRelationAdapterWithOrdering.apply(keyValuePairs);
								keyValuePairs.forEach(entry -> originalRelationFixer.apply(bean, entry));
							} // else : no association record
						});
					}
				}).then(inMemoryRelationHolderClearer));
		
		return result;
	}
	
	/**
	 * Factorizes the logic shared by key and value entities when a map relation is already joined through the
	 * association table: adds a join to collect the entity in-memory, and gives back an adapter that reads back
	 * the entity from its raw identifier, along with the {@link SelectListener}s that must initialize/clear the
	 * in-memory holder, and the resulting {@link GraftPoint}.
	 */
	private <SRC, SRCID, RAW, ENTITY_ID, ENTITY, MAPTABLE extends Table<MAPTABLE>, ENTITYTABLE extends Table<ENTITYTABLE>>
	EntityRelationAdapter<SRC, SRCID, RAW, ENTITY> appendJoinedEntityRelation(
			EntityJoinTree<SRC, SRCID> aggregateTree,
			String mapJoinNodeName,
			ReadWritePropertyAccessPoint<SRC, ?> mapAccessor,
			ConfiguredEntityReader<ENTITY, ENTITY_ID, ENTITYTABLE> entityReader,
			MapMemberAsEntity<ENTITY, ENTITY_ID, MAPTABLE, ENTITYTABLE, ?> entityDefinition,
			ConfiguredEntityReader<SRC, SRCID, ?> sourcePersister,
			Function<KeyValueRecord<?, ?, SRCID>, ENTITY_ID> rawIdExtractor) {
		
		// we keep the link between id and entity found through the join and then use it to build the final map
		InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY> inMemoryEntityRelationHolder = new InMemoryRelationHolder<>();
		
		// the final map is made of the entities found in the in-memory relation holder
		BiFunction<SRCID, RAW, ENTITY> adapter = (srcid, rawValue) ->
				nullable(Iterables.find(inMemoryEntityRelationHolder.giveEntityEntries(srcid), duo -> duo.getLeft().equals(rawValue))).map(InMemoryRelationHolder.MapEntry::getRight).get();
		
		// we ask for our own relation holder to be initialized and cleared
		SelectListener<SRC, SRCID> initializer = new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryEntityRelationHolder.init();
			}
		};
		SelectListener<SRC, SRCID> clearer = new SelectListener<SRC, SRCID>() {
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				inMemoryEntityRelationHolder.clear();
			}
		};
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				inMemoryEntityRelationHolder.clear();
			}
		});
		
		String entityJoinNodeName = appendEntityJoin(aggregateTree, mapJoinNodeName, (ReadWritePropertyAccessPoint) mapAccessor,
				entityReader, entityDefinition.getForeignKey(), inMemoryEntityRelationHolder, rawIdExtractor);
		GraftPoint graftPoint = new GraftPoint(entityDefinition.getEntity(), entityReader, entityJoinNodeName);
		
		return new EntityRelationAdapter<>(adapter, initializer, clearer, graftPoint);
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
	                        ConfiguredEntityReader<ENTITY, ENTITY_ID, ENTITYTABLE> entityPersister,
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
	
	private static class EntityRelationAdapter<SRC, SRCID, RAW, ENTITY> {
		
		private final BiFunction<SRCID, RAW, ENTITY> adapter;
		private final SelectListener<SRC, SRCID> initializer;
		private final SelectListener<SRC, SRCID> clearer;
		private final GraftPoint graftPoint;
		
		private EntityRelationAdapter(BiFunction<SRCID, RAW, ENTITY> adapter, SelectListener<SRC, SRCID> initializer, SelectListener<SRC, SRCID> clearer, GraftPoint graftPoint) {
			this.adapter = adapter;
			this.initializer = initializer;
			this.clearer = clearer;
			this.graftPoint = graftPoint;
		}
	}
}

