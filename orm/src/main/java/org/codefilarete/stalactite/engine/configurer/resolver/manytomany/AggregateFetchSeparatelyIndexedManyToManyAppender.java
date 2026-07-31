package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalIndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Indexed counterpart of {@link AggregateFetchSeparatelyManyToManyAppender} : loads an ordered many-to-many relation in 2
 * phases, the second-phase query being rooted on the indexed association table and joining the target entities, which
 * are gathered per index so that the collection is filled in the order given by the index column of the association
 * table.
 * <p>
 * Note that the 2-phase load is not a lazy loading : no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 * @see AssociationTableLoader
 * @see AggregateJoinedIndexedManyToManyAppender
 */
public class AggregateFetchSeparatelyIndexedManyToManyAppender {
	
	/**
	 * Registers the second phase of the load onto the source persister: once the owning entities are loaded, a single
	 * query, built from the {@link EntityJoinTree} of a dedicated {@link AssociationTableLoader}, selects the
	 * association records joined with their target entities from the identifiers of the owning entities. Target
	 * entities are gathered in memory, per index, while the result set is read, then sewn onto their owner.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * 		   loaded by that very same second-phase query
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint = relation.getAccessor();
		ASSOCIATIONTABLE associationTable = join.getJoinTable();
		IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new IndexedAssociationRecordMapping<>(
				associationTable,
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
				associationTable.getLeftIdentifierColumnMapping(),
				associationTable.getRightIdentifierColumnMapping());
		
		Map<JoinLink<LEFTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> sourcePkToAssociationTableKey =
				new KeyMapping<>(join.getLeftKey(), join.getLeftAssociationKey()).getMapping();
		
		AssociationTableLoader<IndexedAssociationRecord, IndexedAssociationRecord, SRC, SRCID, LEFTTABLE, ASSOCIATIONTABLE> associationRecordLoader =
				new AssociationTableLoader<>(
						sourcePersister.getMapping().getIdMapping(),
						associationRecordMapping,
						sourcePkToAssociationTableKey,
						dialect,
						connectionProvider);
		
		// the target entity is joined onto the loader's tree so that association records and target entities are
		// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
		// since the owner is not part of that query.
		ThreadLocalIndexedRelationStorage<SRCID, TRGT> targetEntityHolder = new ThreadLocalIndexedRelationStorage<>();
		String targetJoinName = associationRecordLoader.getEntityJoinTree().addRelationJoin(
				ROOT_JOIN_NAME,
				new EntityMappingAdapter<>(targetPersister.getMapping()),
				(PropertyAccessPoint) collectionAccessPoint,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				(BeanRelationFixer<IndexedAssociationRecord, TRGT>) (record, target) ->
						targetEntityHolder.storeRelation((SRCID) record.getLeft(), record.getIndex(), target),
				Collections.emptySet(),
				null);
		
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				try {
					targetEntityHolder.init();
					
					Map<SRCID, ? extends SRC> sourcePerId = Iterables.map(result, sourcePersister.getMapping()::getId);
					
					// loading the association records: target entities are collected in memory by the join relation fixer
					associationRecordLoader.select(sourcePerId.keySet());
					
					// we sew the relations
					sourcePerId.forEach((srcId, src) -> {
						// targetPerIndex can be null if there's no associated entity in the database
						Map<Integer, TRGT> targetPerIndex = targetEntityHolder.giveRelatedEntities(srcId);
						if (targetPerIndex != null) {
							S relationCollection = collectionAccessPoint.get(src);
							if (relationCollection == null) {
								relationCollection = relation.getComponentFactory().get();
								collectionAccessPoint.set(src, relationCollection);
							}
							// the values() are sorted thanks to the Map sorted on the index
							relationCollection.addAll(targetPerIndex.values());
						}
					});
				} finally {
					// we remove the internal ThreadLocal
					targetEntityHolder.clear();
				}
			}
		});
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the aggregate one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetPersister, targetJoinName, associationRecordLoader.getEntityJoinTree());
	}
}
