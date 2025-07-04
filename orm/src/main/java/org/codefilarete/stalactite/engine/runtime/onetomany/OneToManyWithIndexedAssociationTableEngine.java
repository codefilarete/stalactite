package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.IndexedDiff;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecordInsertionCascader;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.codefilarete.tool.collection.Iterables.minus;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedAssociationTableEngine<
		SRC,
		TRGT,
		SRCID,
		TRGTID,
		C extends Collection<TRGT>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, IndexedAssociationRecord, ASSOCIATIONTABLE> {

	/**
	 * Context for indexed mapped List. Will keep bean index during select between "unrelated" methods/phases:
	 * index column must be added to SQL select, read from ResultSet and order applied to sort final List, but this particular feature crosses over
	 * layers (entities and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non-static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<SRCID, Map<TRGTID, Integer>>> currentSelectedIndexes = new ThreadLocal<>();

	/** Column that stores index value */
	private final Column<ASSOCIATIONTABLE, Integer> indexColumn;
	
	public OneToManyWithIndexedAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
													  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
													  ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
													  AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> associationPersister,
													  Column<ASSOCIATIONTABLE, Integer> indexColumn,
													  WriteOperationFactory writeOperationFactory) {
		super(sourcePersister, targetPersister, manyRelationDescriptor, associationPersister, writeOperationFactory);
		this.indexColumn = indexColumn;
	}
	
	@Override
	public void addSelectCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister, boolean loadSeparately) {
		
		// we join on the association table and add bean association in memory
		String associationTableJoinNodeName = sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				associationPersister.getMainTable().getOneSideKey(),
				associationPersister.getMainTable().getOneSideForeignKey(),
				JoinType.OUTER,
				Arrays.asHashSet(indexColumn));
		
		// we add target subgraph joins to main persister
		String rightEntityJoinName = targetPersister.joinAsMany(sourcePersister,
				associationPersister.getMainTable().getManySideForeignKey(),
				associationPersister.getMainTable().getManySideKey(), manyRelationDescriptor.getRelationFixer(),
				columnedRow -> {
					TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
					// indexColumn column value is took on join of association table, not target table, so we have to grab it
					JoinNode<Fromable> join = sourcePersister.getEntityJoinTree().getJoin(associationTableJoinNodeName);
					ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(join);
					Integer targetEntityIndex = rowDecoder.get(indexColumn);
					return identifier + "-" + targetEntityIndex;
				}, associationTableJoinNodeName, true, loadSeparately);

		addIndexSelection(associationTableJoinNodeName, rightEntityJoinName);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> collect = Iterables.stream(result).flatMap(src -> nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
						.map(Collection::stream)
						.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
	}

	private void addIndexSelection(String associationTableJoinNodeName, String rightEntityJoinName) {
		// Implementation note: we keep the object indexes and put the sorted entities in a temporary Collection, then add them all to the target List
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				currentSelectedIndexes.set(new HashMap<>());
				InMemoryRelationHolder relationFixer = (InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer();
				relationFixer.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				InMemoryRelationHolder relationFixer = (InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer();
				relationFixer.applySort(result);
				cleanContext();
			}

			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				cleanContext();
			}

			private void cleanContext() {
				currentSelectedIndexes.remove();
				InMemoryRelationHolder relationFixer = (InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer();
				relationFixer.clear();
			}
		});
		AbstractJoinNode<TRGT, Fromable, Fromable, TRGTID> join = (AbstractJoinNode<TRGT, Fromable, Fromable, TRGTID>) sourcePersister.getEntityJoinTree().getJoin(rightEntityJoinName);
		join.setConsumptionListener((trgt, columnValueProvider) -> {
			SRCID leftEntityId = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(EntityTreeInflater.currentContext().getDecoder(sourcePersister.getEntityJoinTree().getRoot()));
			int index = EntityTreeInflater.currentContext().getDecoder(sourcePersister.getEntityJoinTree().getJoin(associationTableJoinNodeName)).get(indexColumn);
			((InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer()).addIndex(leftEntityId, trgt, index);
		});
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved, boolean maintainAssociationOnly, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		// NB: we don't have any reverseSetter (for applying source entity to reverse side (target entity)), because this is only relevant
		// when association is mapped without intermediary table (owned by "many-side" entity)
		CollectionUpdater<SRC, TRGT, C> collectionUpdater = new CollectionUpdater<SRC, TRGT, C>(manyRelationDescriptor.getCollectionGetter(), targetPersister, null, shouldDeleteRemoved) {
			
			@Override
			protected AssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
				return new AssociationTableUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onHeldElements(updateContext, diff);
				IndexedDiff indexedDiff = (IndexedDiff) diff;
				Set<Integer> minus = minus(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
				Integer index = first(minus);
				if (index != null ) {
					SRC leftIdentifier = updateContext.getPayload().getLeft();
					PairIterator<Integer, Integer> diffIndexIterator = new PairIterator<>(indexedDiff.getReplacerIndexes(), indexedDiff.getSourceIndexes());
					diffIndexIterator.forEachRemaining(d -> {
						if (!d.getLeft().equals(d.getRight()))
							((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated().add(new Duo<>(
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getLeft()),
									newRecord(leftIdentifier, diff.getSourceInstance(), d.getRight())));
					});
				}
			}
			
			@Override
			protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
				return getDiffer().diffOrdered(unmodified, modified);
			}
			
			@Override
			protected void onAddedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onAddedElements(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getReplacerIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(
								newRecord(leftIdentifier, diff.getReplacingInstance(), idx)));
			}
			
			@Override
			protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onRemovedElements(updateContext, diff);
				SRC leftIdentifier = updateContext.getPayload().getLeft();
				((IndexedDiff<TRGT>) diff).getSourceIndexes().forEach(idx ->
						((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(
								newRecord(leftIdentifier, diff.getSourceInstance(), idx)));
			}
			
			@Override
			protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
				super.updateTargets(updateContext, allColumnsStatement);
				// association records can't be updated because they are primary key elements for us (see EmbeddedClassMapping and its updatableProperties computation), 
				// so we ask for their deletion + creation
				List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> associationRecordsToBeUpdated = ((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated();
				associationPersister.delete(Iterables.collectToList(associationRecordsToBeUpdated, Duo::getRight));
				associationPersister.insert(Iterables.collectToList(associationRecordsToBeUpdated, Duo::getLeft));
			}
			
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				// we insert targets before association records to satisfy integrity constraint
				super.insertTargets(updateContext);
				associationPersister.insert(((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted());
				
			}
			
			@Override
			protected void deleteTargets(UpdateContext updateContext) {
				// we delete association records before targets to satisfy integrity constraint
				associationPersister.delete(((AssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted());
				super.deleteTargets(updateContext);
			}
			
			class AssociationTableUpdateContext extends UpdateContext {
				
				private final List<IndexedAssociationRecord> associationRecordsToBeInserted = new ArrayList<>();
				private final List<IndexedAssociationRecord> associationRecordsToBeDeleted = new ArrayList<>();
				private final List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> associationRecordsToBeUpdated = new ArrayList<>();
				
				public AssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordsToBeInserted() {
					return associationRecordsToBeInserted;
				}
				
				public List<IndexedAssociationRecord> getAssociationRecordsToBeDeleted() {
					return associationRecordsToBeDeleted;
				}
				
				public List<Duo<IndexedAssociationRecord, IndexedAssociationRecord>> getAssociationRecordsToBeUpdated() {
					return associationRecordsToBeUpdated;
				}
			}
		};
		
		// Can we cascade update on target entities ? it depends on relation maintenance mode
		if (!maintainAssociationOnly) {
			persisterListener.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
		}
	}
	
	@Override
	protected IndexedAssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> associationPersister,
			EntityMapping<SRC, SRCID, ?> mappingStrategy,
			EntityMapping<TRGT, TRGTID, ?> targetStrategy) {
		return new IndexedAssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected IndexedAssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new IndexedAssociationRecord(sourcePersister.getMapping().getId(e), targetPersister.getMapping().getId(target), index);
	}
}