package org.codefilarete.stalactite.engine.runtime.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.IndexedDiff;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedMappedManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyWithAssociationTableEngine.INDEXED_COLLECTION_FIRST_INDEX_VALUE;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C, RIGHTTABLE> {
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column<RIGHTTABLE, Integer> indexColumn;

	public OneToManyWithIndexedMappedAssociationEngine(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
													   IndexedMappedManyRelationDescriptor<SRC, TRGT, C, SRCID, TRGTID> manyRelationDefinition,
													   ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
													   Set<Column<RIGHTTABLE, ?>> mappedReverseColumns,
													   Column<RIGHTTABLE, Integer> indexColumn,
													   Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider) {
		super(targetPersister, manyRelationDefinition, sourcePersister, mappedReverseColumns, reverseColumnsValueProvider);
		this.indexColumn = indexColumn;
	}
	
	@Override
	public IndexedMappedManyRelationDescriptor<SRC, TRGT, C, SRCID, TRGTID> getManyRelationDescriptor() {
		return (IndexedMappedManyRelationDescriptor<SRC, TRGT, C, SRCID, TRGTID>) manyRelationDescriptor;
	}
	
	@Override
	public <T1 extends Table<T1>, T2 extends Table<T2>> String addSelectCascade(Key<T1, SRCID> sourcePrimaryKey,
																				boolean loadSeparately) {
		// we add target subgraph joins to main persister
		Set<Column<RIGHTTABLE, ?>> columnsToSelect = new HashSet<>(targetPersister.<RIGHTTABLE>getMainTable().getPrimaryKey().getColumns());
		columnsToSelect.add(indexColumn);
		String relationJoinNodeName = targetPersister.joinAsMany(EntityJoinTree.ROOT_JOIN_NAME, sourcePersister, manyRelationDescriptor.getCollectionProvider(), sourcePrimaryKey, (Key<RIGHTTABLE, SRCID>) manyRelationDescriptor.getReverseColumn(),
				manyRelationDescriptor.getRelationFixer(),
				(columnedRow) -> {
					TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
					Integer targetEntityIndex = columnedRow.get(indexColumn);
					return identifier + "-" + targetEntityIndex;
				},
				columnsToSelect,
				true,
				loadSeparately);
		
		addIndexSelection(relationJoinNodeName);
		
		// we must trigger subgraph event on loading of our own graph, this is mainly for event that initializes things because given ids
		// are not those of their entity
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
		
		return relationJoinNodeName;
	}
	
	private void addIndexSelection(String joinNodeName) {
		// Implementation note: 2 possibilities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List through a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems a bit more complex.
		// May be changed if any performance issue is noticed
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
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
				InMemoryRelationHolder relationFixer = (InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer();
				relationFixer.clear();
			}
		});
		AbstractJoinNode<TRGT, Fromable, Fromable, TRGTID> join = (AbstractJoinNode<TRGT, Fromable, Fromable, TRGTID>) sourcePersister.getEntityJoinTree().getJoin(joinNodeName);
		join.setConsumptionListener((trgt, columnValueProvider) -> {
			InMemoryRelationHolder relationFixer = (InMemoryRelationHolder) manyRelationDescriptor.getRelationFixer();
			Map<TRGTID, Integer> indexPerTargetId = relationFixer.getCurrentSelectedIndexes();
			// indexPerTargetId may not be present because its mechanism was added on persisterListener which is the one of the source bean
			// so in case of entity loading from its own persister (targetPersister) ThreadLocal is not available
			if (indexPerTargetId != null) {
				// Indexing column is not defined in targetPersister.getMapping().getRowTransformer() but is present in row
				// because it was read from ResultSet
				int index = columnValueProvider.get(indexColumn);
				TRGTID relationOwnerId = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnValueProvider);
				indexPerTargetId.put(relationOwnerId, index);
			}
		});
	}
	
	@Override
	public void addInsertCascade(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		// For a List and a given manner to get its owner (so we can deduce index value), we configure persistence to keep index value in database
		addIndexInsertion();
		super.addInsertCascade(targetPersister);
	}
	
	/**
	 * Adds a "listener" that will amend insertion of the index column filled with its value
	 */
	private <TARGETTABLE extends Table<TARGETTABLE>> void addIndexInsertion() {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		Function<SRC, C> collectionGetter = this.manyRelationDescriptor.getCollectionGetter();
		ShadowColumnValueProvider<TRGT, TARGETTABLE> indexValueProvider = new ShadowColumnValueProvider<TRGT, TARGETTABLE>() {
			@Override
			public boolean accept(TRGT entity) {
				// NB: source entity must be taken on relation storage context, not through manyRelationDefinition.getReverseGetter()
				// because it doesn't cover all cases, moreover relation storage context is maintained for foreign key
				// management which is make more sense that index strategy rely on it too
				SRC sourceEntity = giveRelationStorageContext().get(entity);
				return
						// Source entity can be null if target was removed from the collection, then an SQL update is required to set its reference
						// column to null as well as its indexing column
						sourceEntity == null
								// Case of polymorphic inheritance with an abstract one-to-many relation redefined on each subclass (think to
								// AbstractQuestion and mapOneToMany(AbstractQuestion::getChoices, ..) declared for single and multiple choice question):
								// we get several source persister which are quite the same at a slighlty difference on collection getter : due to JVM
								// serialization of method reference, it keeps original generic type somewhere in the serialized form of method reference
								// (SerializedLambda.instantiatedMethodType), then applying this concrete class (when looking for target entity index in
								// collection) and not the abstract one, which produces a ClassCastException. As a consequence we must check that
								// collection getter matches given entity (which is done through source persister, because there's no mean to do it
								// with collection getter).
								|| sourcePersister.getMapping().getClassToPersist().isInstance(sourceEntity);
			}
			
			@Override
			public Set<Column<TARGETTABLE, ?>> getColumns() {
				return Collections.singleton((Column) indexColumn);
			}
			
			@Override
			public Map<Column<TARGETTABLE, ?>, ?> giveValue(TRGT target) {
				SRC source = giveRelationStorageContext().get(target);
				Integer targetEntityIndex;
				if (source == null) {
					// index can be null if target entity has been removed from source, no exception to be thrown here
					// since it's a normal case
					targetEntityIndex = null;
				} else {
					targetEntityIndex = computeTargetIndex(source, target);
				}
				Map<Column<TARGETTABLE, ?>, Object> result = new HashMap<>();
				result.put((Column) indexColumn, targetEntityIndex);
				return result;
			}
			
			/**
			 * Finds the index of target instance in the one-to-many collection of source entity.
			 * Supports {@link List} and {@link LinkedHashSet} collection type. Else an exception is thrown.
			 * 
			 * @param source an entity that owns the one-to-many relation
			 * @param target an entity expected to be in one-to-many relation
			 * @return the index of target instance in one-to-many relation
			 */
			private int computeTargetIndex(SRC source, TRGT target) {
				int result;
				C apply = collectionGetter.apply(source);
				if (apply instanceof List) {
					result = ((List<?>) apply).indexOf(target) + INDEXED_COLLECTION_FIRST_INDEX_VALUE;
				} else if (apply instanceof LinkedHashSet) {
					MutableInt counter = new MutableInt(INDEXED_COLLECTION_FIRST_INDEX_VALUE - 1);
					for (Object o : apply) {
						counter.increment();
						if (o == target) {
							break;
						}
					}
					result = counter.getValue();
				} else {
					throw new UnsupportedOperationException("Index computation is not supported for " + Reflections.toString(apply.getClass()));
				}
				return result;
			}
		};
		
		targetPersister.<TARGETTABLE>getMapping().addShadowColumnInsert(indexValueProvider);
		targetPersister.<TARGETTABLE>getMapping().addShadowColumnUpdate(indexValueProvider);
	}
	
	@Override
	protected void addTargetInstancesUpdateCascader(boolean shouldDeleteRemoved) {
		BiConsumer<Duo<SRC, SRC>, Boolean> collectionUpdater = new ListCollectionUpdater<>(
				this.manyRelationDescriptor.getCollectionGetter(),
				this.targetPersister,
				this.manyRelationDescriptor.getReverseSetter(),
				shouldDeleteRemoved,
				this.targetPersister.getMapping()::getId,
				indexColumn);
		sourcePersister.getPersisterListener().addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
	}
	
	private static class ListCollectionUpdater<SRC, TRGT, ID, C extends Collection<TRGT>, TARGETTABLE extends Table<TARGETTABLE>> extends CollectionUpdater<SRC, TRGT, C> {
		
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
		private final Column<TARGETTABLE, Integer> indexingColumn;
		
		private ListCollectionUpdater(Function<SRC, C> collectionGetter,
									  ConfiguredRelationalPersister<TRGT, ID> targetPersister,
									  @Nullable BiConsumer<TRGT, SRC> reverseSetter,
									  boolean shouldDeleteRemoved,
									  Function<TRGT, ?> idProvider,
									  Column<TARGETTABLE, Integer> indexingColumn) {
			super(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved, idProvider);
			this.indexingColumn = indexingColumn;
			addShadowIndexInsert(targetPersister);
			addShadowIndexUpdate(targetPersister);
			
		}
		
		private void addShadowIndexUpdate(ConfiguredRelationalPersister<TRGT, ID> targetPersister) {
			targetPersister.<TARGETTABLE>getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, TARGETTABLE>() {
				
				@Override
				public boolean accept(TRGT entity) {
					return currentUpdatableListIndex.get() != null && currentUpdatableListIndex.get().containsKey(entity);
				}
				
				@Override
				public Set<Column<TARGETTABLE, ?>> getColumns() {
					return Arrays.asHashSet(indexingColumn);
				}
				
				@Override
				public Map<Column<TARGETTABLE, ?>, ?> giveValue(TRGT bean) {
					Map<Column<TARGETTABLE, ?>, Object> result = new HashMap<>();
					result.put(indexingColumn, currentUpdatableListIndex.get().get(bean));
					return result;
				}
			});
		}
		
		private void addShadowIndexInsert(ConfiguredRelationalPersister<TRGT, ID> targetPersister) {
			// adding index insert/update to strategy
			targetPersister.<TARGETTABLE>getMapping().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, TARGETTABLE>() {
				
				@Override
				public boolean accept(TRGT entity) {
					return currentInsertableListIndex.get() != null && currentInsertableListIndex.get().containsKey(entity);
				}
				
				@Override
				public Set<Column<TARGETTABLE, ?>> getColumns() {
					return Arrays.asHashSet(indexingColumn);
				}
				
				@Override
				public Map<Column<TARGETTABLE, ?>, ?> giveValue(TRGT bean) {
					Map<Column<TARGETTABLE, ?>, Object> result = new HashMap<>();
					result.put(indexingColumn, currentInsertableListIndex.get().get(bean));
					return result;
				}
			});
		}
		
		@Override
		protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
			return getDiffer().diffOrdered(unmodified, modified);
		}
		
		@Override
		protected UpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
			return new IndexedMappedAssociationUpdateContext(updatePayload);
		}
		
		@Override
		protected void onAddedElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
			super.onAddedElements(updateContext, diff);
			addNewIndexToContext((IndexedDiff<TRGT>) diff, (IndexedMappedAssociationUpdateContext) updateContext);
		}
		
		@Override
		protected void onHeldElements(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
			super.onHeldElements(updateContext, diff);
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
