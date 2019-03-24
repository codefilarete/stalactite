package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.structure.Column;

import static org.gama.lang.collection.Iterables.collectToList;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends List<TRGT>>
		extends OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<TRGT, Integer>> updatableListIndex = new ThreadLocal<>();
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column indexingColumn;
	
	public OneToManyWithIndexedMappedAssociationEngine(Persister<TRGT, TRGTID, ?> targetPersister,
													   IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition,
													   JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister,
													   Column indexingColumn) {
		super(targetPersister, manyRelationDefinition, joinedTablesPersister);
		this.indexingColumn = indexingColumn;
	}
	
	@Override
	public void addSelectCascade(Column sourcePrimaryKey, Column relationshipOwner) {
		super.addSelectCascade(sourcePrimaryKey, relationshipOwner);
		addIndexSelection();
	}
	
	private void addIndexSelection() {
		targetPersister.getSelectExecutor().getMappingStrategy().addSilentColumnSelecter(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
		// May be changed if any performance issue is noticed
		persisterListener.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				updatableListIndex.set(new HashMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(i -> {
						List<TRGT> apply = manyRelationDefinition.getCollectionGetter().apply(i);
						apply.sort(Comparator.comparingInt(TRGT -> updatableListIndex.get().get(TRGT)));
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
				updatableListIndex.remove();
			}
		});
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
			Map<TRGT, Integer> indexPerBean = updatableListIndex.get();
			// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
			// because it was read from ResultSet
			// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
			Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
			indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
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
		targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(indexingColumn, (Function<TRGT, Object>)
				target -> {
					IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition =
							(IndexedMappedManyRelationDescriptor<SRC, TRGT, C>) this.manyRelationDefinition;
					SRC source = manyRelationDefinition.getReverseGetter().apply(target);
					if (source == null) {
						throw new RuntimeMappingException("Can't get index : " + target + " is not associated with a "
								+ Reflections.toString(joinedTablesPersister.getMappingStrategy().getClassToPersist()) + " : "
								// NB: we let Mutator print itself because it has a self defined toString()
								+ manyRelationDefinition.getReverseGetterSignature() + " returned null");
					}
					return this.manyRelationDefinition.getCollectionGetter().apply(source).indexOf(target);
				});
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		
		targetPersister.getMappingStrategy().addSilentColumnUpdater(indexingColumn,
				// Thread safe by updatableListIndex access
				(Function<TRGT, Object>) TRGT -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(TRGT)).get());
		
		BiConsumer<UpdatePayload<? extends SRC, ?>, Boolean> updateListener = new CollectionUpdater<SRC, TRGT, C>(
				manyRelationDefinition.getCollectionGetter(),
				targetPersister,
				manyRelationDefinition.getReverseSetter(),
				shouldDeleteRemoved) {
			
			@Override
			protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
				return getDiffer().diffList((List<TRGT>) unmodified, (List<TRGT>) modified);
			}
			
			@Override
			protected UpdateContext newUpdateContext(UpdatePayload<? extends SRC, ?> updatePayload) {
				return new IndexedMappedAssociationUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<TRGT> diff) {
				super.onHeldTarget(updateContext, diff);
				Set<Integer> minus = Iterables.minus(((IndexedDiff<TRGT>) diff).getReplacerIndexes(), ((IndexedDiff<TRGT>) diff).getSourceIndexes());
				Integer index = Iterables.first(minus);
				if (index != null) {
					((IndexedMappedAssociationUpdateContext) updateContext).getIndexUpdates().put(diff.getReplacingInstance(), index);
				}
			}
			
			@Override
			protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
				// we ask for entities update, code seems weird because we pass a Duo of same instance which should update nothing because
				// entities are stricly equal, but in fact this update() invokation triggers the "silent column" update, which is the index column
				ThreadLocals.doWithThreadLocal(updatableListIndex, ((IndexedMappedAssociationUpdateContext) updateContext)::getIndexUpdates,
						(Runnable) () -> {
							List<Duo<? extends TRGT, ? extends TRGT>> entities = collectToList(updatableListIndex.get().keySet(), TRGT -> new Duo<>(TRGT, TRGT));
							targetPersister.update(entities, false);
						});
			}
			
			class IndexedMappedAssociationUpdateContext extends UpdateContext {
				
				/** New indexes per entity */
				private final Map<TRGT, Integer> indexUpdates = new HashMap<>();
				
				public IndexedMappedAssociationUpdateContext(UpdatePayload<? extends SRC, ?> updatePayload) {
					super(updatePayload);
				}
				
				public Map<TRGT, Integer> getIndexUpdates() {
					return indexUpdates;
				}
			}
		};
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
}
