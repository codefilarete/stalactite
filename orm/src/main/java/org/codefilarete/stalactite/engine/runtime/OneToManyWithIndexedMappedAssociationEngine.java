package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.RuntimeMappingException;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.IndexedDiff;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.query.builder.IdentityMap;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index during select between "unrelated" methods/phases :
	 * index column must be added to SQL select, read from ResultSet and order applied to sort final List, but this particular feature crosses over
	 * layers (entities and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non-static (acceptable small overhead)
	 */
	private final ThreadLocal<IdentityMap<TRGTID, Integer>> currentSelectedIndexes = new ThreadLocal<>();
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column<Table, Integer> indexingColumn;
	
	public OneToManyWithIndexedMappedAssociationEngine(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
													   IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition,
													   EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
													   Column<? extends Table, Integer> indexingColumn) {
		super(targetPersister, manyRelationDefinition, sourcePersister);
		this.indexingColumn = (Column<Table, Integer>) indexingColumn;
	}
	
	@Override
	public void addSelectCascade(Column sourcePrimaryKey,
								 Column relationOwner,    // foreign key on target table
								 boolean loadSeparately) {
		// we add target subgraph joins to main persister
		Column<Table, TRGTID> primaryKey = (Column<Table, TRGTID>) Iterables.first(targetPersister.getMapping().getTargetTable().getPrimaryKey().getColumns());
		String joinNodeName = targetPersister.joinAsMany(sourcePersister, sourcePrimaryKey, relationOwner, manyRelationDescriptor.getRelationFixer(),
				(row, columnedRow) -> {
					TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
					Integer targetEntityIndex = columnedRow.getValue(indexingColumn, row);
					return identifier + "-" + targetEntityIndex;
				},
				EntityJoinTree.ROOT_STRATEGY_NAME,
				Arrays.asHashSet(indexingColumn, primaryKey), relationOwner.isNullable(),
				loadSeparately);
		
		addIndexSelection(joinNodeName, primaryKey);
		
		// we must trigger subgraph event on loading of our own graph, this is mainly for event that initializes things because given ids
		// are not those of their entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				Iterable collect = Iterables.stream(result).flatMap(src -> org.codefilarete.tool.Nullable.nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
						.map(Collection::stream)
						.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	private void addIndexSelection(String joinNodeName, Column<Table, TRGTID> primaryKey) {
		// Implementation note: 2 possibilities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List through a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems a bit more complex.
		// May be changed if any performance issue is noticed
		sourcePersister.getPersisterListener().addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				currentSelectedIndexes.set(new IdentityMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(src -> {
						List<TRGT> apply = manyRelationDescriptor.getCollectionGetter().apply(src);
						apply.sort(Comparator.comparingInt(target -> currentSelectedIndexes.get().get(targetPersister.getId(target))));
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				currentSelectedIndexes.remove();
			}
		});
		AbstractJoinNode<TRGT, Table, Table, TRGTID> join = (AbstractJoinNode<TRGT, Table, Table, TRGTID>) sourcePersister.getEntityJoinTree().getJoin(joinNodeName);
		join.setTransformerListener((trgt, rowValueProvider) -> {
			IdentityMap<TRGTID, Integer> indexPerBean = currentSelectedIndexes.get();
			// indexPerBean may not be present because its mechanism was added on persisterListener which is the one of the source bean
			// so in case of entity loading from its own persister (targetPersister) ThreadLocal is not available
			if (indexPerBean != null) {
				// Indexing column is not defined in targetPersister.getMapping().getRowTransformer() but is present in row
				// because it was read from ResultSet
				int index = EntityTreeInflater.currentContext().giveValue(joinNodeName, indexingColumn);
				TRGTID relationOwnerId = (TRGTID) EntityTreeInflater.currentContext().giveValue(joinNodeName, primaryKey);
				indexPerBean.put(relationOwnerId, index);
			}
		});
	}
	
	@Override
	public void addInsertCascade() {
		// For a List and a given manner to get its owner (so we can deduce index value), we configure persistence to keep index value in database
		addIndexInsertion();
		super.addInsertCascade();
	}
	
	/**
	 * Adds a "listener" that will amend insertion of the index column filled with its value
	 */
	private void addIndexInsertion() {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition =
				(IndexedMappedManyRelationDescriptor<SRC, TRGT, C>) this.manyRelationDescriptor;
		Function<SRC, C> collectionGetter = this.manyRelationDescriptor.getCollectionGetter();
		targetPersister.getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, Integer, Table>(indexingColumn, target -> {
			SRC source = manyRelationDefinition.getReverseGetter().apply(target);
			if (source == null) {
				throw new RuntimeMappingException("Can't get index : " + target + " is not associated with a "
						+ Reflections.toString(sourcePersister.getMapping().getClassToPersist()) + " : "
						// NB: we let Mutator print itself because it has a self defined toString()
						+ manyRelationDefinition.getReverseGetterSignature() + " returned null");
			}
			return collectionGetter.apply(source).indexOf(target);
		}) {
			@Override
			public boolean accept(TRGT entity) {
				SRC sourceEntity = manyRelationDefinition.getReverseGetter().apply(entity);
				return 
						// Source entity can be null if target was removed from the collection, then an SQL update is required to set its reference
						// column to null as well as its indexing column
						sourceEntity == null
						// Case of polymorphic inheritance with an abstract one-to-many relation redefined on each subclass (think to
						// AbstractQuestion and mapOneToManyList(AbstractQuestion::getChoices, ..) declared for single and multiple choice question):
						// we get several source persister which are quite the same at a slighlty difference on collection getter : due to JVM
						// serialization of method reference, it keeps original generic type somewhere in the serialized form of method reference
						// (SerializedLambda.instantiatedMethodType), then applying this concrete class (when looking for target entity index in
						// collection) and not the abstract one, which produces a ClassCastException. As a consequence we must check that
						// collection getter matches given entity (which is done through source persister, because there's no mean to do it
						// with collection getter).
						|| sourcePersister.getMapping().getClassToPersist().isInstance(sourceEntity);
			}
		});
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new ListCollectionUpdater<>(
				this.manyRelationDescriptor.getCollectionGetter(),
				this.targetPersister,
				this.manyRelationDescriptor.getReverseSetter(),
				shouldDeleteRemoved,
				this.targetPersister.getMapping()::getId,
				this.indexingColumn);
		sourcePersister.getPersisterListener().addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	private static class ListCollectionUpdater<SRC, TRGT, ID, C extends List<TRGT>> extends CollectionUpdater<SRC, TRGT, C> {
		
		/**
		 * Context for indexed mapped List. Will keep bean index during insert between "unrelated" methods/phases :
		 * indexes must be computed then applied into SQL order, but this particular feature crosses over layers (entities
		 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
		 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non static (acceptable small overhead)
		 */
		@SuppressWarnings("java:S5164" /* remove() is called by ThreadLocals.AutoRemoveThreadLocal */)
		private final ThreadLocal<Map<TRGT, Integer>> currentUpdatableListIndex = new ThreadLocal<>();
		
		/**
		 * Context for indexed mapped List. Will keep bean index during update between "unrelated" methods/phases :
		 * indexes must be computed then applied into SQL order, but this particular feature crosses over layers (entities
		 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
		 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non static (acceptable small overhead)
		 */
		@SuppressWarnings("java:S5164" /* remove() is called by ThreadLocals.AutoRemoveThreadLocal */)
		private final ThreadLocal<Map<TRGT, Integer>> currentInsertableListIndex = new ThreadLocal<>();
		private final Column indexingColumn;
		
		private ListCollectionUpdater(Function<SRC, C> collectionGetter,
									 EntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
									 @Nullable BiConsumer<TRGT, SRC> reverseSetter,
									 boolean shouldDeleteRemoved,
									 Function<TRGT, ?> idProvider,
									 Column indexingColumn) {
			super(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved, idProvider);
			this.indexingColumn = indexingColumn;
			addShadowIndexInsert(targetPersister);
			addShadowIndexUpdate(targetPersister);
			
		}
		
		private void addShadowIndexUpdate(EntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister) {
			targetPersister.getMapping().addShadowColumnUpdate(new ShadowColumnValueProvider<TRGT, Object, Table>(
					indexingColumn,
					// Thread safe by updatableListIndex access
					target -> currentUpdatableListIndex.get().get(target)) {
				@Override
				public boolean accept(Object entity) {
					return currentUpdatableListIndex.get() != null && currentUpdatableListIndex.get().containsKey(entity);
				}
			});
		}
		
		private void addShadowIndexInsert(EntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister) {
			// adding index insert/update to strategy
			targetPersister.getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, Object, Table>(
					indexingColumn,
					// Thread safe by updatableListIndex access
					target ->
							this.currentInsertableListIndex.get().get(target)) {
				@Override
				public boolean accept(Object entity) {
					return currentInsertableListIndex.get() != null && currentInsertableListIndex.get().containsKey(entity);
				}
			});
		}
		
		@Override
		protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
			return getDiffer().diffList((List<TRGT>) unmodified, (List<TRGT>) modified);
		}
		
		@Override
		protected UpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
			return new IndexedMappedAssociationUpdateContext(updatePayload);
		}
		
		@Override
		protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
			super.onAddedTarget(updateContext, diff);
			addNewIndexToContext((IndexedDiff<TRGT>) diff, (IndexedMappedAssociationUpdateContext) updateContext);
		}
		
		@Override
		protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
			super.onHeldTarget(updateContext, diff);
			addNewIndexToContext((IndexedDiff<TRGT>) diff, (IndexedMappedAssociationUpdateContext) updateContext);
		}
		
		private void addNewIndexToContext(IndexedDiff<TRGT> diff, IndexedMappedAssociationUpdateContext updateContext) {
			Set<Integer> minus = Iterables.minus(diff.getReplacerIndexes(), diff.getSourceIndexes());
			Integer index = Iterables.first(minus);
			if (index != null) {
				updateContext.getIndexUpdates().put(diff.getReplacingInstance(), index);
			}
		}
		
		@Override
		protected void insertTargets(UpdateContext updateContext) {
			// we ask for entities insert as super does but we surround it by a ThreadLocal to fulfill List indexes which is required by
			// the shadow column inserter (List indexes are given by default CollectionUpdater algorithm)
			ThreadLocals.doWithThreadLocal(currentInsertableListIndex, ((IndexedMappedAssociationUpdateContext) updateContext)::getIndexUpdates,
					(Runnable) () -> super.insertTargets(updateContext));
		}
		
		@Override
		protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
			// we ask for entities update as super does but we surround it by a ThreadLocal to fulfill List indexes which is required by
			// the shadow column updater (List indexes are given by default CollectionUpdater algorithm)
			ThreadLocals.doWithThreadLocal(currentUpdatableListIndex, ((IndexedMappedAssociationUpdateContext) updateContext)::getIndexUpdates,
					(Runnable) () -> super.updateTargets(updateContext, allColumnsStatement));
		}
		
		class IndexedMappedAssociationUpdateContext extends UpdateContext {
			
			/** New indexes per entity */
			private final Map<TRGT, Integer> indexUpdates = new IdentityHashMap<>();
			
			public IndexedMappedAssociationUpdateContext(Duo<SRC, SRC> updatePayload) {
				super(updatePayload);
			}
			
			public Map<TRGT, Integer> getIndexUpdates() {
				return indexUpdates;
			}
		}
	}
	
}
