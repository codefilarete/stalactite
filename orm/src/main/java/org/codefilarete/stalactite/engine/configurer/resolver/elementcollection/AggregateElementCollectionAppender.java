package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
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
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateElementCollectionAppender {
	
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
					
					Set<ElementRecord<TRGT, SRCID>> select = elementCollectionLoader.select(srcIds);
					
					// we sow the relations
					ReadWritePropertyAccessPoint<SRC, S> mapAccessPoint = relation.getAccessor();
					result.forEach(src -> {
						// filling final collection with a sorted collection
						S relationCollection = mapAccessPoint.get(src);
						if (relationCollection == null) {
							relationCollection = relation.getComponentFactory().get();
							mapAccessPoint.set(src, relationCollection);
						}
						// the values() are sorted thanks to the Map with Integer as key
						relationCollection.addAll(select.stream().filter(record -> Objects.equals(record.getId(), sourcePersister.getMapping().getId(src)))
								.map(ElementRecord::getElement)
								.collect(Collectors.toList()));
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
