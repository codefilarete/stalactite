package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
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

/**
 * Wires a {@link Map} relation with some 2-phase load: as a difference with {@link JoinedMapAppender}, the association
 * table is not joined onto the aggregate's {@link EntityJoinTree}, but a dedicated loader ({@link MapEntryLoader})
 * performs a second SELECT once the owning entities have been loaded. Key and/or value entities, when applicable, are
 * joined onto that second-phase tree and collected in-memory so the raw identifiers of the loaded
 * {@link KeyValueRecord}s can be replaced by the actual entities before the {@link Map} is set onto the owner.
 * 
 * @author Guillaume Mary
 * @see JoinedMapAppender
 */
public class FetchSeparatelyMapAppender {
	
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
		
		Duo<GraftPoint /* key assembly point */, GraftPoint /* value assembly point */> result = new Duo<>();
		
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
					Collection<InMemoryRelationHolder<SRCID, ENTITY_ID, ENTITY>.MapEntry> duos = inMemoryEntityHolder.giveEntityEntries(record.getId().getId());
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
}

