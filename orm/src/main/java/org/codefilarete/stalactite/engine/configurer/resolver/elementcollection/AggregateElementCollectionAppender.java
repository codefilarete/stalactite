package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.elementcollection.IndexedElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateElementCollectionAppender {
	
	public static final Comparator<ElementRecord<?, ?>> INDEXED_RECORD_COMPARATOR = Comparator.comparing(o -> ((IndexedElementRecord) o).getIndex());
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>>
	void append(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> relation,
	            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	            ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister,
	            String mountPoint,
	            EntityJoinTree<SRC, SRCID> aggregateTree,
				Dialect dialect,
				ConnectionProvider connectionProvider) {
		
		DirectRelationJoin<LEFTTABLE, COLLECTIONTABLE, SRCID> join = relation.getJoin();
		if (relation.isFetchSeparately()) {
			// adding second phase loader
			KeyMapping<LEFTTABLE, COLLECTIONTABLE, SRCID> targetPkToRightKey = new KeyMapping<>(sourcePersister.getMapping().getTargetTable().getPrimaryKey(), join.getRightKey());
			KeepOrderMap<JoinLink<LEFTTABLE, ?>, JoinLink<COLLECTIONTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
			
			ElementCollectionLoader<SRC, SRCID, TRGT, LEFTTABLE, COLLECTIONTABLE> elementCollectionLoader = new ElementCollectionLoader<>(
					sourcePersister.getMapping().getIdMapping(),
					collectionPersister,
					targetPkToAssociationTableKey,
					dialect,
					connectionProvider);
			
			// Adding a listener that loads the entries after the main entities
			// Note that the selector may be a wrapper that combine the initial raw results (entities identifiers) with the real entities kept in memory
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Set<SRCID> srcIds = Iterables.collect(result, sourcePersister.getMapping()::getId, HashSet::new);
					Set<ElementRecord<TRGT, SRCID>> collectionsRecords = elementCollectionLoader.select(srcIds);
					
					Map<SRCID, Collection<ElementRecord<TRGT, SRCID>>> collectionBySourceId = new HashMap<>();
					Stream<ElementRecord<TRGT, SRCID>> elementRecordStream;
					if (relation.isOrdered()) {
						// we sort all the retrieved records by their position, mixing source identifiers, it doesn't
						// matter since collectionBySourceId is built by iterating over them in this order
						elementRecordStream = collectionsRecords.stream()
								.sorted(INDEXED_RECORD_COMPARATOR);
					} else {
						elementRecordStream = collectionsRecords.stream();
					}
					elementRecordStream.forEach(record ->
						collectionBySourceId.computeIfAbsent(record.getId(),
										// let's keep track of addition order in case of sorted collection
										k -> new LinkedHashSet<>())
								.add(record)
					);
					
					// we sew the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						Collection<ElementRecord<TRGT, SRCID>> trgtCollection = collectionBySourceId.get(sourcePersister.getMapping().getId(src));
						if (trgtCollection != null) {
							// the trgtCollection is sorted thanks to the elementRecordStream
							trgtCollection.forEach(target -> relation.getRelationFixer().apply(src, target));
						}
					});
				}
			});
			
		} else {
			aggregateTree.addRelationJoin(
					mountPoint,
					new EntityMappingAdapter<>(collectionPersister.getMapping()),
					relation.getAccessor(),
					join.getLeftKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),
					Collections.emptySet(),
					null);
		}
	}
}
