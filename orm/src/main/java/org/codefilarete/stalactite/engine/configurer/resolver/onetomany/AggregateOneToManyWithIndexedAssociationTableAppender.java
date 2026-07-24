package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseIndexedRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.IndexedRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyWithIndexedAssociationTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                            EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                            EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                            String mountPoint,
	                                            EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		return appendIndexedAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint);
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendIndexedAssociation(EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                    EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                    ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                    PropertyAccessor<SRC, S> accessor,
	                                    EntityJoinTree<SRC, SRCID> aggregateTree,
	                                    String mountPoint) {
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = (Column<ASSOCIATIONTABLE, Integer>) relation.<IndexedAssociationTable>getIndexingAssociationColumn();
		ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint = relation.getAccessor();
		
		if (relation.isFetchSeparately()) {
			ThreadLocal<IndexedRelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
			
			// We build a function capable of building the identifier from the association table columns, because,
			// if we give the targetPersister identifier assembler then the runtime fails : identifier takes its values
			// from the target table columns which are missins in the join : the join only contains the right
			// table columns and the association ones (that's separate-load principle)
			KeyMapping<RIGHTTABLE, ASSOCIATIONTABLE, TRGTID> targetPkToRightKey = join.getRightKey().reference(join.getRightAssociationKey());
			KeepOrderMap<JoinLink<RIGHTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
			Function<ColumnedRow, TRGTID> idMapping = columnedRow -> targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(new ColumnedRow() {
				@Override
				public <E> E get(Selectable<E> pkColumn) {
					return (E) columnedRow.get(targetPkToAssociationTableKey.get(pkColumn));
				}
			});
			
			// here is the logic below :
			// - we collect the SRC-Index-TRGTID on the association join: see FirstPhaseIndexedRelationLoader usage hereafter
			// - then we trigger the target entities collect on the afterSelect of the source
			// - just after, we can apply the relation
			aggregateTree.addMergeJoin(mountPoint,
					new FirstPhaseIndexedRelationLoader<SRC, TRGTID>(idMapping, (Set) join.getRightAssociationKey().getColumns(), indexingColumn,
							current2PhasesLoadContext),
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER);
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					current2PhasesLoadContext.set(new IndexedRelationStorage<>());
				}

				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Map<SRC, Map<Integer, TRGTID>> targetIdPerIndexPerSource = current2PhasesLoadContext.get().getTargetIdPerIndexPerSource();
					Set<TRGTID> trgtids = targetIdPerIndexPerSource.values().stream().flatMap(m -> m.values().stream()).collect(Collectors.toSet());
					Set<TRGT> targets = targetPersister.select(trgtids);
					Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetPersister.getMapping()::getId));
					
					// we sow the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						Map<Integer, TRGTID> targetIdPerIndex = targetIdPerIndexPerSource.get(src);
						if (targetIdPerIndex != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
							S relationCollection = collectionAccessPoint.get(src);
							if (relationCollection == null) {
								relationCollection = relation.getComponentFactory().get();
								collectionAccessPoint.set(src, relationCollection);
							}
							// the values() are sorted thanks to the Map with Integer as key
							Map<Integer, TRGT> targetPerIndex = Maps.innerJoinOnValuesAndKeys(targetIdPerIndex, targetPerId);
							relationCollection.addAll(targetPerIndex.values());
						}
					});
					
					clearContext();
				}

				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					clearContext();
				}

				private void clearContext() {
					current2PhasesLoadContext.remove();
				}
			});
			
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
			return new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
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
			
			String manyJoinName = aggregateTree.addRelationJoin(
					associationTableJoinName,
					new EntityMappingAdapter<>(targetPersister.getMapping()),
					accessor,
					join.getRightAssociationKey(),
					join.getRightKey(),
					null,
					OUTER,
					inMemoryRelationFixer,
					Collections.emptySet(),
					duplicateIdentifierProvider);
			
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
}
