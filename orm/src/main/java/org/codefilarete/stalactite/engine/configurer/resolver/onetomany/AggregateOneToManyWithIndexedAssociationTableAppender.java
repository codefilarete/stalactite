package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseIndexedManyRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseIndexedManyRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalIndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Wires an ordered one-to-many relation owned by an indexed association table onto the aggregate's
 * {@link EntityJoinTree}.
 * <p>
 * Two strategies are available, depending on {@link ResolvedOneToManyRelation#isFetchSeparately()}:
 * <ul>
 * <li>the joined one: the association table and the target table are joined onto the aggregate's tree, hence
 * everything is loaded by the very same query</li>
 * <li>the fetch-separately one (2-phase load): the aggregate's tree is left untouched, so the main query doesn't
 * return the cartesian product of all the aggregate relations. Instead, a dedicated {@link AssociationTableLoader},
 * which owns its own {@link EntityJoinTree} rooted on the association table, selects the association records joined
 * with their target entities from the identifiers of the already-loaded owning entities. Target entities are then
 * sewn onto their owner in the order given by the index column of the association table.</li>
 * </ul>
 * Note that the 2-phase load is not a lazy loading: no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 * @see AggregateOneToManyAppender
 * @see AggregateOneToManyWithAssociationTableAppender
 * @see AssociationTableLoader
 */
public class AggregateOneToManyWithIndexedAssociationTableAppender {
	
	private final PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = (Column<ASSOCIATIONTABLE, Integer>) relation.<IndexedAssociationTable>getIndexingAssociationColumn();
		ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint = relation.getAccessor();
		
		if (relation.isFetchSeparately()) {
			// Note that the aggregate tree is not modified at all: the association table is the root of a dedicated
			// tree owned by the loader below, which prevents the main query from returning too many rows
			return appendWithSeparateFetch(sourcePersister, targetPersister, relation, join, mountPoint, aggregateTree, dialect, connectionProvider);
		} else {
			Holder<String> associationTableJoinNodeNameHolder = new Holder<>();
			Function<ColumnedRow, Object> duplicateIdentifierProvider = (columnedRow) -> {
				TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
				// indexColumn column value is took on join of association table, not target table, so we have to grab it
				JoinNode<IndexedAssociationRecord, Fromable> joinNode = (JoinNode<IndexedAssociationRecord, Fromable>) aggregateTree.getJoin(associationTableJoinNodeNameHolder.get());
				ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(joinNode);
				Integer targetEntityIndex = rowDecoder.get(indexingColumn);
				return identifier + "-" + targetEntityIndex;
			};
			
			// we join on the association table
			String associationTableJoinName = aggregateTree.addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					// we must add all the columns to make them available while decoding the row to create an IndexedAssociationRecord
					join.getJoinTable().getColumns());
			associationTableJoinNodeNameHolder.set(associationTableJoinName);
			
			// Implementation note: we keep the object indexes and put the sorted entities in a temporary Collection, then add them all to the target List
			InMemoryRelationHolder<SRC, SRCID, TRGT, S> inMemoryRelationFixer = new InMemoryRelationHolder<>(
					sourcePersister.getMapping()::getId,
					collectionAccessPoint,
					relation.getComponentFactory(),
					relation.getMappedByAccessor()
			);
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryRelationFixer.init();
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryRelationFixer.applySort(result);
					cleanContext();
				}
				
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					cleanContext();
				}
				
				private void cleanContext() {
					inMemoryRelationFixer.clear();
				}
			});
			
			String manyJoinName;
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				GraftableRelation<SRC, TRGT, TRGTID, ASSOCIATIONTABLE, RIGHTTABLE, TRGTID> graftableRelation = new GraftableRelation<>(
								join.getRightAssociationKey(),
								join.getRightKey(),
								collectionAccessPoint,
								(BeanRelationFixer<Object, TRGT>) inMemoryRelationFixer,
								relation.getTargetEntity());
				
				GraftPoint graftPoint = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetPersister,
						graftableRelation,
						associationTableJoinName);
				
				manyJoinName = graftPoint.getParentJoinPoint();
			} else {
				manyJoinName = aggregateTree.addRelationJoin(
						associationTableJoinName,
						new EntityMappingAdapter<>(targetPersister.getMapping()),
						collectionAccessPoint,
						join.getRightAssociationKey(),
						join.getRightKey(),
						null,
						OUTER,
						inMemoryRelationFixer,
						Collections.emptySet(),
						duplicateIdentifierProvider);
			}
			
			JoinNode<TRGT, Fromable> joinNode = (JoinNode<TRGT, Fromable>) aggregateTree.getJoin(manyJoinName);
			JoinNode<?, Fromable> associationJoinNode = aggregateTree.getJoin(associationTableJoinName);
			IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new IndexedAssociationRecordMapping<>(
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
	 * Registers the second phase of the load onto the source persister: once the owning entities are loaded, a single
	 * query, built from the {@link EntityJoinTree} of a dedicated {@link AssociationTableLoader}, selects the indexed
	 * association records joined with their target entities from the identifiers of the owning entities. Target
	 * entities are gathered in memory, per owner identifier and per index, while the result set is read, then sewn
	 * onto their owner in the order given by the index column.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * loaded by that very same second-phase query
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendWithSeparateFetch(ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
									   ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
									   ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
									   IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join,
									   String mountPoint,
									   EntityJoinTree<SRC, SRCID> aggregateTree,
									   Dialect dialect,
									   ConnectionProvider connectionProvider) {
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
		
		// the target entity is joined onto the loader's tree so that association records and target entities are
		// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
		// since the owner is not part of that query.
		if (relation.getTargetEntity() instanceof PolymorphicEntity) {
			PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
			ThreadLocalIndexedRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalIndexedRelationStorage<>();
			
			EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
			
			String associationTableJoinName = sourceReader.getEntityJoinTree().addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					Collections.singleton(associationTable.getIndexColumn()),
					null);
			
			JoinNode<?, ?> associationJoinNode = sourceReader.getEntityJoinTree().getJoin(associationTableJoinName);
			
			polymorphicSkeletonAppender.appendForSeparateLoad(
					sourceReader.getEntityJoinTree(),
					polymorphism,
					targetReader,
					associationTableJoinName,
					new BiFunction<Function<ColumnedRow, TRGTID>, Set<? extends Selectable<?>>, EntityMerger<SRC>>() {
						@Override
						public EntityMerger<SRC> apply(Function<ColumnedRow, TRGTID> columnedRowTRGTIDFunction, Set<? extends Selectable<?>> selectables) {
							return new FirstPhaseIndexedManyRelationLoader<>(columnedRowTRGTIDFunction,
									selectables,
									current2PhasesLoadContext,
									row -> EntityTreeInflater.currentContext()
											.getDecoder(associationJoinNode)
											.get(associationTable.getIndexColumn()));
						}
					},
					new KeyMapping<>(join.getRightAssociationKey(), join.getRightKey()));
			
			sourceReader.addSelectListener(new SecondPhaseIndexedManyRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		} else {
			ThreadLocalIndexedRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalIndexedRelationStorage<>();
			Function<ColumnedRow, TRGTID> associationTableRecordAssembler = row -> {
				AssociationRecord associationRecord = associationRecordMapping.getIdMapping().getIdentifierAssembler().assemble(row);
				return associationRecord == null ? null : (TRGTID) associationRecord.getRight();
			};
			// we get all the columns of the association table to have the target id columns and make the target assembler able to assemble its id
			Set<Column<ASSOCIATIONTABLE, ?>> associationTableColumns = join.getLeftAssociationKey().getTable().getColumns();
			FirstPhaseIndexedManyRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseIndexedManyRelationLoader<>(
					associationTableRecordAssembler,
					associationTableColumns,
					current2PhasesLoadContext,
					associationTable.getIndexColumn());
			aggregateTree.addMergeJoin(mountPoint,
					firstPhaseCollector,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER);
			
			// adding second phase loader
			sourceReader.addSelectListener(new SecondPhaseIndexedManyRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		}
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the given as argument one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, associationRecordLoader.getEntityJoinTree());
	}
}
