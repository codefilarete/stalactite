package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalIndexedRelationStorage;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToManyRelation}.
 * <p>
 * Since many-to-many relations always use an intermediary association table, two strategies are available, depending
 * on {@link ResolvedManyToManyRelation#isFetchSeparately()}:
 * <ul>
 * <li>the joined one: a passive join from the source table to the association table, then a relation join from the
 * association table to the target table, both onto the aggregate's tree, hence everything is loaded by the very same
 * query</li>
 * <li>the fetch-separately one (2-phase load): the aggregate's tree is left untouched, so the main query doesn't
 * return the cartesian product of all the aggregate relations. Instead, a dedicated {@link AssociationTableLoader},
 * which owns its own {@link EntityJoinTree} rooted on the association table, selects the association records joined
 * with their target entities from the identifiers of the already-loaded owning entities. Target entities are then
 * sewn onto their owner.</li>
 * </ul>
 * When the association table carries an index column ({@link IndexedAssociationTable}), collection order is preserved:
 * either through an {@link InMemoryRelationHolder} used as relation fixer (joined strategy), or through a
 * {@link ThreadLocalIndexedRelationStorage} filled while the second-phase result set is read (fetch-separately one).
 * <p>
 * Note that the 2-phase load is not a lazy loading: no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 * @see AssociationTableLoader
 */
public class AggregateManyToManyAppender {
	
	/**
	 * Appends the given many-to-many relation to the aggregate persister by:
	 * <ol>
	 *   <li>Delegating write-cascade setup to {@link ManyToManyResolver}.</li>
	 *   <li>Adding the two necessary join segments to the root persister's join tree.</li>
	 *   <li>Forwarding SELECT lifecycle events from the source persister to the target persister.</li>
	 * </ol>
	 *
	 * @return an {@link GraftPoint} for the target entity, ready to be pushed onto the assembly queue
	 * so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                                        EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                        EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                                        String mountPoint,
	                                                        EntityJoinTree<SRC, SRCID> aggregateTree,
	                                                        Dialect dialect,
	                                                        ConnectionProvider connectionProvider) {
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		GraftPoint result;
		if (relation.isOrdered()) {
			result = appendIndexedAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint, dialect, connectionProvider);
		} else {
			result = appendAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint, dialect, connectionProvider);
		}
		
		// Forward SELECT lifecycle events from the source entity's persister down to the target persister
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> targets = Nullable.nullable(result)
						.map(r -> r.stream()
								.flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
										.map(Collection::stream)
										.getOr(Stream.empty()))
								.collect(Collectors.toSet()))
						.getOr(Collections.emptySet());
				targetSelectListener.afterSelect(targets);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
		
		return result;
	}
	
	/**
	 * Adds two join segments for the non-indexed association-table case:
	 * <ol>
	 *   <li>Passive join from source table to the association table.</li>
	 *   <li>Relation join from the association table to the target table, using the pre-built
	 *       {@link org.codefilarete.stalactite.sql.result.BeanRelationFixer} (which encodes optional
	 *       bidirectionality).</li>
	 * </ol>
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                             EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                             ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                             PropertyAccessor<SRC, S> accessor,
	                             EntityJoinTree<SRC, SRCID> entityJoinTree,
	                             String mountPoint,
	                             Dialect dialect,
	                             ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		
		if (relation.isFetchSeparately()) {
			// Note that the aggregate tree is not modified at all: the association table is the root of a dedicated
			// tree owned by the loader below, which prevents the main query from returning too many rows
			return appendSeparatelyFetchedAssociation(sourcePersister, targetPersister, relation, join, dialect, connectionProvider);
		} else {
			String associationTableJoinName = entityJoinTree.addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					Collections.emptySet());
			
			String manyJoinName = entityJoinTree.addRelationJoin(
					associationTableJoinName,
					new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
					accessor,
					join.getRightAssociationKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),   // pre-built fixer handles bidirectionality if configured
					Collections.emptySet(),
					null);
			return new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, entityJoinTree);
		}
	}
	
	/**
	 * Registers the second phase of the load onto the source persister: once the owning entities are loaded, a single
	 * query, built from the {@link EntityJoinTree} of a dedicated {@link AssociationTableLoader}, selects the
	 * association records joined with their target entities from the identifiers of the owning entities. Target
	 * entities are gathered in memory while the result set is read, then sewn onto their owner.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * 		   loaded by that very same second-phase query
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendSeparatelyFetchedAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                              EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                              ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                              IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join,
	                                              Dialect dialect,
	                                              ConnectionProvider connectionProvider) {
		ASSOCIATIONTABLE associationTable = join.getJoinTable();
		AssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new AssociationRecordMapping<>(
				associationTable,
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				targetPersister.getMapping().getIdMapping().getIdentifierAssembler());
		
		Map<JoinLink<LEFTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> sourcePkToAssociationTableKey =
				new KeyMapping<>(join.getLeftKey(), join.getLeftAssociationKey()).getMapping();
		
		AssociationTableLoader<AssociationRecord, AssociationRecord, SRC, SRCID, LEFTTABLE, ASSOCIATIONTABLE> associationRecordLoader =
				new AssociationTableLoader<>(
						sourcePersister.getMapping().getIdMapping(),
						associationRecordMapping,
						sourcePkToAssociationTableKey,
						dialect,
						connectionProvider);
		
		// the target entity is joined onto the loader's tree so that association records and target entities are
		// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
		// since the owner is not part of that query.
		ThreadLocalRelationStorage<SRCID, TRGT> targetEntityHolder = new ThreadLocalRelationStorage<>();
		String targetJoinName = associationRecordLoader.getEntityJoinTree().addRelationJoin(
				ROOT_JOIN_NAME,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
				(PropertyAccessPoint) relation.getAccessor(),
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				(BeanRelationFixer<AssociationRecord, TRGT>) (record, target) -> targetEntityHolder.storeRelation((SRCID) record.getLeft(), target),
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
						Collection<TRGT> targets = targetEntityHolder.giveRelatedEntities(srcId);
						if (targets != null) {
							targets.forEach(target -> relation.getRelationFixer().apply(src, target));
						}
					});
				} finally {
					// we remove the internal ThreadLocal
					targetEntityHolder.clear();
				}
			}
		});
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the given as argument one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetPersister, targetJoinName, associationRecordLoader.getEntityJoinTree());
	}
	
	/**
	 * Adds two join segments for the indexed association-table case.
	 * An {@link InMemoryRelationHolder} is used as the relation fixer so that collection ordering is
	 * restored in-memory after the flat SQL result is accumulated.
	 */
	@SuppressWarnings("unchecked")
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendIndexedAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                PropertyAccessor<SRC, S> accessor,
	                                EntityJoinTree<SRC, SRCID> aggregateTree,
	                                String mountPoint,
	                                Dialect dialect,
	                                ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = join.getJoinTable().getIndexColumn();
		ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint = relation.getAccessor();
		
		if (relation.isFetchSeparately()) {
			// Note that the aggregate tree is not modified at all: the association table is the root of a dedicated
			// tree owned by the loader below, which prevents the main query from returning too many rows
			return appendSeparatelyFetchedIndexedAssociation(sourcePersister, targetPersister, relation, join, collectionAccessPoint, dialect, connectionProvider);
		} else {
			Holder<String> associationTableJoinNodeNameHolder = new Holder<>();
			Function<ColumnedRow, Object> duplicateIdentifierProvider = columnedRow -> {
				TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
				JoinNode<IndexedAssociationRecord, Fromable> joinNode =
						(JoinNode<IndexedAssociationRecord, Fromable>) aggregateTree
								.getJoin(associationTableJoinNodeNameHolder.get());
				ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(joinNode);
				Integer targetEntityIndex = rowDecoder.get(indexingColumn);
				return identifier + "-" + targetEntityIndex;
			};
			
			// Passive join: source table → association table (include all columns for index decoding)
			String associationTableJoinName = aggregateTree.addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					join.getJoinTable().getColumns());
			associationTableJoinNodeNameHolder.set(associationTableJoinName);
			
			// The InMemoryRelationHolder buffers entities with their index and applies the sorted order after select
			// Note: getMappedByAccessor() is null in model.ManyToManyRelation; bidirectionality on the read path is a
			// known limitation for indexed M2M (consistent with indexed OneToMany with association table).
			InMemoryRelationHolder<SRC, SRCID, TRGT, S> inMemoryRelationFixer = new InMemoryRelationHolder<>(
					sourcePersister.getMapping()::getId,
					collectionAccessPoint,
					relation.getComponentFactory(),
					null);
			
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryRelationFixer.init();
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryRelationFixer.applySort(result);
					inMemoryRelationFixer.clear();
				}
				
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					inMemoryRelationFixer.clear();
				}
			});
			
			// Relation join: association table → target table
			String manyJoinName = aggregateTree.addRelationJoin(
					associationTableJoinName,
					new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
					accessor,
					join.getRightAssociationKey(),
					join.getRightKey(),
					null,
					OUTER,
					inMemoryRelationFixer,
					Collections.emptySet(),
					duplicateIdentifierProvider);
			
			// Attach a consumption listener: each time a target row is consumed, record its position from the association table row
			JoinNode<TRGT, Fromable> joinNode = (JoinNode<TRGT, Fromable>) aggregateTree.getJoin(manyJoinName);
			JoinNode<?, Fromable> associationJoinNode = aggregateTree.getJoin(associationTableJoinName);
			IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping =
					new IndexedAssociationRecordMapping<>(
							join.getJoinTable(),
							sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
							targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
							join.getJoinTable().getLeftIdentifierColumnMapping(),
							join.getJoinTable().getRightIdentifierColumnMapping());
			joinNode.setConsumptionListener((trgt, columnValueProvider) -> {
				ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(associationJoinNode);
				IndexedAssociationRecord associationRecord = associationRecordMapping.getRowTransformer().transform(rowDecoder);
				inMemoryRelationFixer.addIndex((SRCID) associationRecord.getLeft(), trgt, associationRecord.getIndex());
			});
			
			return new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, aggregateTree);
		}
	}
	
	/**
	 * Indexed counterpart of {@link #appendSeparatelyFetchedAssociation}: the second-phase query is rooted on the
	 * indexed association table and joins the target entities, which are gathered per index so that the collection is
	 * filled in the order given by the index column of the association table.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * 		   loaded by that very same second-phase query
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendSeparatelyFetchedIndexedAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                     EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                                     ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                                     IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join,
	                                                     ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint,
	                                                     Dialect dialect,
	                                                     ConnectionProvider connectionProvider) {
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
				new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
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
		// not the given as argument one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetPersister, targetJoinName, associationRecordLoader.getEntityJoinTree());
	}
}
