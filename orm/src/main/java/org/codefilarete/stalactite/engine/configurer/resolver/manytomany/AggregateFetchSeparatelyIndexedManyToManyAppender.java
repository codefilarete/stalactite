package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseIndexedManyRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseIndexedManyRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalIndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
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
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
					  String mountPoint,
					  EntityJoinTree<SRC, SRCID> aggregateTree,
					  Dialect dialect,
					  ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		ASSOCIATIONTABLE associationTable = join.getJoinTable();
		IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new IndexedAssociationRecordMapping<>(
				associationTable,
				sourceReader.getMapping().getIdMapping().getIdentifierAssembler(),
				targetReader.getMapping().getIdMapping().getIdentifierAssembler(),
				associationTable.getLeftIdentifierColumnMapping(),
				associationTable.getRightIdentifierColumnMapping());
		
		Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<ASSOCIATIONTABLE, ?>> sourcePkToAssociationTableKey =
				new KeyMapping<>(join.getLeftKey(), join.getLeftAssociationKey()).getMapping();
		
		AssociationTableLoader<IndexedAssociationRecord, IndexedAssociationRecord, SRC, SRCID, LEFTTABLE, ASSOCIATIONTABLE> associationRecordLoader =
				new AssociationTableLoader<>(
						sourceReader.getMapping().getIdMapping(),
						associationRecordMapping,
						sourcePkToAssociationTableKey,
						dialect,
						connectionProvider);
		
		ThreadLocalIndexedRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalIndexedRelationStorage<>();
		if (relation.getTargetEntity() instanceof PolymorphicEntity) {
			// we add a listener on the source reader to trigger the second phase of the load once the source entities are loaded
			sourceReader.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					Map<SRCID, ? extends SRC> sourcePerId = Iterables.map(result, sourceReader.getMapping()::getId);
					// getting the relation record with their index through the association table loader
					Set<IndexedAssociationRecord> loadedAssociationRecords = associationRecordLoader.select(sourcePerId.keySet());
					Map<SRCID, SortedSet<IndexedAssociationRecord>> sortedAssociationRecords = new HashMap<>();
					loadedAssociationRecords.forEach(associationRecord -> {
						SRCID sourceId = (SRCID) associationRecord.getLeft();
						// we sort the records by their index to make it iterable in the right order when sewing the target entities onto their owner
						sortedAssociationRecords.computeIfAbsent(sourceId, k -> new TreeSet<>(Comparator.comparingInt(IndexedAssociationRecord::getIndex)))
								.add(associationRecord);
					});
					
					Set<TRGTID> targetIds = Iterables.collect(loadedAssociationRecords, associationRecord -> (TRGTID) associationRecord.getRight(), HashSet::new);
					// we load the target entities in a single query, since they are polymorphic and thus cannot be loaded by a single query
					Set<TRGT> targetEntities = targetReader.select(targetIds);
					Map<TRGTID, TRGT> targetPerId = Iterables.map(targetEntities, targetReader.getMapping()::getId);
					// sewing the relations : we iterate over the association records sorted by their index, so that the collection is filled in the right order
					sortedAssociationRecords.forEach((sourceId, associationRecords) -> {
						SRC source = sourcePerId.get(sourceId);
						associationRecords.forEach(associationRecord -> {
							TRGT target = targetPerId.get(associationRecord.getRight());
							relation.getRelationFixer().apply(source, target);
						});
					});
				}
			});
		} else {
			Function<ColumnedRow, TRGTID> associationTableRecordAssembler = row -> {
				AssociationRecord associationRecord = associationRecordMapping.getIdMapping().getIdentifierAssembler().assemble(row);
				return associationRecord == null ? null : (TRGTID) associationRecord.getRight();
			};
			// the target entity is joined onto the loader's tree so that association records and target entities are
			// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
			// since the owner is not part of that query.
			// we get all the columns of the association table to have the target id columns and make the target assembler able to assemble its id
			Set<Column<ASSOCIATIONTABLE, ?>> associationTableColumns = join.getLeftAssociationKey().getTable().getColumns();
			FirstPhaseIndexedManyRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseIndexedManyRelationLoader<>(
					associationTableRecordAssembler,
					associationTableColumns,
					current2PhasesLoadContext,
					join.getJoinTable().getIndexColumn());
			aggregateTree.addMergeJoin(mountPoint,
					firstPhaseCollector,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER);
			
			// adding second phase loader
			sourceReader.addSelectListener(new SecondPhaseIndexedManyRelationSelectListener<>(
					firstPhaseCollector,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		}
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the aggregate one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, associationRecordLoader.getEntityJoinTree());
	}
}
