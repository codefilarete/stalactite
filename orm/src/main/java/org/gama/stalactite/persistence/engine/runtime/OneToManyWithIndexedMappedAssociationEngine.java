package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.ShadowColumnValueProvider;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;
import org.gama.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index during select between "unrelated" methods/phases :
	 * index column must be added to SQL select, read from ResultSet and order applied to sort final List, but this particular feature crosses over
	 * layers (entities and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<IdentityMap<TRGT, Integer>> currentSelectedIndexes = new ThreadLocal<>();
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column indexingColumn;
	
	public OneToManyWithIndexedMappedAssociationEngine(IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
													   IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition,
													   IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
													   Column indexingColumn) {
		super(targetPersister, manyRelationDefinition, sourcePersister);
		this.indexingColumn = indexingColumn;
	}
	
	@Override
	public void addSelectCascade(Column sourcePrimaryKey,
								 Column relationOwner    // foreign key on target table
	) {
		// we add target subgraph joins to main persister
		targetPersister.joinAsMany(sourcePersister, sourcePrimaryKey, relationOwner, manyRelationDescriptor.getRelationFixer(),
				new BiFunction<Row, ColumnedRow, Object>() {
					@Override
					public Object apply(Row row, ColumnedRow columnedRow) {
						TRGTID identifier = targetPersister.getMappingStrategy().getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow);
						Number targetEntityIndex = (Number) columnedRow.getValue(indexingColumn, row);
						return identifier + "-" + targetEntityIndex;
						
					}
				}, EntityJoinTree.ROOT_STRATEGY_NAME, relationOwner.isNullable());
		
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
				Iterable collect = Iterables.stream(result).flatMap(src -> org.gama.lang.Nullable.nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
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
		addIndexSelection();
	}
	
	private void addIndexSelection() {
		// NB: should be "targetPersister.getSelectExecutor().getMappingStrategy().." but can't be due to interface ISelectExecutor
		targetPersister.getMappingStrategy().addShadowColumnSelect(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
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
						apply.sort(Comparator.comparingInt(target -> currentSelectedIndexes.get().get(target)));
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
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().addTransformerListener((bean, rowValueProvider) -> {
			IdentityMap<TRGT, Integer> indexPerBean = currentSelectedIndexes.get();
			// indexPerBean may not be present because its mecanism was added on persisterListener which is the one of the source bean
			// so in case of entity loading from its own persister (targetPersister) ThreadLocal is not available
			if (indexPerBean != null) {
				// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
				// because it was read from ResultSet
				indexPerBean.put(bean, (int) rowValueProvider.apply(indexingColumn));
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
		targetPersister.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, Object, Table>(indexingColumn, target -> {
			SRC source = manyRelationDefinition.getReverseGetter().apply(target);
			if (source == null) {
				throw new RuntimeMappingException("Can't get index : " + target + " is not associated with a "
						+ Reflections.toString(sourcePersister.getMappingStrategy().getClassToPersist()) + " : "
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
						// AbstractQuestion and addOneToManyList(AbstractQuestion::getChoices, ..) declared for single and multiple choice question):
						// we get several source persister which are quite the same at a slighlty difference on collection getter : due to JVM
						// serialization of method reference, it keeps original generic type somewhere in the serialized form of method reference
						// (SerializedLambda.instantiatedMethodType), then applying this concrete class (when looking for target entity index in
						// collection) and not the abstract one, which produces a ClassCastException. As a consequence we must check that
						// collection getter matches given entity (which is done through source persister, because there's no mean to do it
						// with collection getter).
						|| sourcePersister.getMappingStrategy().getClassToPersist().isInstance(sourceEntity);
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
				this.targetPersister.getMappingStrategy()::getId,
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
		private final ThreadLocal<Map<TRGT, Integer>> currentUpdatableListIndex = new ThreadLocal<>();
		
		/**
		 * Context for indexed mapped List. Will keep bean index during update between "unrelated" methods/phases :
		 * indexes must be computed then applied into SQL order, but this particular feature crosses over layers (entities
		 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
		 * Could be static, but would lack the TRGT typing, which leads to some generics errors, so left non static (acceptable small overhead)
		 */
		private final ThreadLocal<Map<TRGT, Integer>> currentInsertableListIndex = new ThreadLocal<>();
		private final Column indexingColumn;
		
		private ListCollectionUpdater(Function<SRC, C> collectionGetter,
									 IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
									 @Nullable BiConsumer<TRGT, SRC> reverseSetter,
									 boolean shouldDeleteRemoved,
									 Function<TRGT, ?> idProvider,
									 Column indexingColumn) {
			super(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved, idProvider);
			this.indexingColumn = indexingColumn;
			addShadowIndexInsert(targetPersister);
			addShadowIndexUpdate(targetPersister);
			
		}
		
		private void addShadowIndexUpdate(IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister) {
			targetPersister.getMappingStrategy().addShadowColumnUpdate(new ShadowColumnValueProvider<TRGT, Object, Table>(indexingColumn,
					// Thread safe by updatableListIndex access
					target -> currentUpdatableListIndex.get().get(target)) {
				@Override
				public boolean accept(Object entity) {
					return currentUpdatableListIndex.get() != null && currentUpdatableListIndex.get().containsKey(entity);
				}
			});
		}
		
		private void addShadowIndexInsert(IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister) {
			// adding index insert/update to strategy
			targetPersister.getMappingStrategy().addShadowColumnInsert(new ShadowColumnValueProvider<TRGT, Object, Table>(indexingColumn,
					// Thread safe by updatableListIndex access
					target -> currentInsertableListIndex.get().get(target)) {
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
