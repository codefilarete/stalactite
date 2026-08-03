package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

/**
 * Wires an ordered many-to-many relation onto the aggregate's {@link EntityJoinTree}, so that target entities are
 * loaded by the very same query as the one that loads the aggregate root : two join segments are added, a passive one
 * from the source table to the association table, then a relation one from the association table to the target table.
 * <p>
 * Because the SQL result is flat and its row order can't be trusted, collection ordering is restored in memory : an
 * {@link InMemoryRelationHolder} is used as relation fixer, and fed with the index column value of the association
 * table through a consumption listener set on the target entity join node. The collection is then sorted once the
 * whole result set is read.
 *
 * @author Guillaume Mary
 * @see AggregateFetchSeparatelyIndexedManyToManyAppender
 */
public class AggregateJoinedIndexedManyToManyAppender {
	
	/**
	 * @return the graft point of the target entity onto the aggregate tree, so that the relations of the target entity
	 * 		   are loaded by that very same query
	 */
	@SuppressWarnings("unchecked")
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  PropertyAccessor<SRC, S> accessor,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = join.getJoinTable().getIndexColumn();
		ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint = relation.getAccessor();
		
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
				new EntityMappingAdapter<>(targetPersister.getMapping()),
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
