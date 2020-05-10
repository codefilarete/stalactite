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
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.structure.Column;

import static org.gama.lang.collection.Iterables.collectToList;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, ID, C extends List<TRGT>>
		extends OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<TRGT, Integer>> updatableListIndex = new ThreadLocal<>();
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column indexingColumn;
	
	public OneToManyWithIndexedMappedAssociationEngine(IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
													   IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition,
													   IEntityConfiguredJoinedTablesPersister<SRC, ID> joinedTablesPersister,
													   Column indexingColumn) {
		super(targetPersister, manyRelationDefinition, joinedTablesPersister);
		this.indexingColumn = indexingColumn;
	}
	
	@Override
	public void addSelectCascade(Column sourcePrimaryKey, Column relationOwner) {
		super.addSelectCascade(sourcePrimaryKey, relationOwner);
		addIndexSelection();
	}
	
	private void addIndexSelection() {
		// NB: should be "targetPersister.getSelectExecutor().getMappingStrategy().." but can't be due to interface ISelectExecutor
		targetPersister.getMappingStrategy().addSilentColumnToSelect(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
		// May be changed if any performance issue is noticed
		sourcePersister.getPersisterListener().addSelectListener(new SelectListener<SRC, ID>() {
			@Override
			public void beforeSelect(Iterable<ID> ids) {
				updatableListIndex.set(new HashMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(src -> {
						List<TRGT> apply = manyRelationDefinition.getCollectionGetter().apply(src);
						apply.sort(Comparator.comparingInt(target -> updatableListIndex.get().get(target)));
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<ID> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				updatableListIndex.remove();
			}
		});
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().addTransformerListener((bean, row) -> {
			Map<TRGT, Integer> indexPerBean = updatableListIndex.get();
			// indexPerBean may not be present because its mecanism was added on persisterListener which is the one of the source bean
			// so in case of entity loading from its own persister (targetPersister) ThreadLocal is not available
			if (indexPerBean != null) {
				// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
				// because it was read from ResultSet
				// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
				// <!> Please note that aliases variable can't be put outside of this loop because aliases are computed lately / lazily when
				// columns are added to select by JoinedStrategySeelct.addColumnsToSelect(..), putting this out is too early
				Map<Column, String> aliases = sourcePersister.getJoinedStrategiesSelect().getAliases();
				indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
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
		targetPersister.getMappingStrategy().addSilentColumnInserter(indexingColumn, (Function<TRGT, Object>)
				target -> {
					IndexedMappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition =
							(IndexedMappedManyRelationDescriptor<SRC, TRGT, C>) this.manyRelationDefinition;
					SRC source = manyRelationDefinition.getReverseGetter().apply(target);
					if (source == null) {
						throw new RuntimeMappingException("Can't get index : " + target + " is not associated with a "
								+ Reflections.toString(sourcePersister.getMappingStrategy().getClassToPersist()) + " : "
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
				(Function<TRGT, Object>) target -> Nullable.nullable(updatableListIndex.get()).map(m -> m.get(target)).get());
		
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new CollectionUpdater<SRC, TRGT, C>(
				manyRelationDefinition.getCollectionGetter(),
				targetPersister,
				manyRelationDefinition.getReverseSetter(),
				shouldDeleteRemoved,
				targetPersister.getMappingStrategy()::getId,
				((MappedManyRelationDescriptor) manyRelationDefinition).getReverseColumn()) {
			
			@Override
			protected Set<? extends AbstractDiff<TRGT>> diff(Collection<TRGT> modified, Collection<TRGT> unmodified) {
				return getDiffer().diffList((List<TRGT>) unmodified, (List<TRGT>) modified);
			}
			
			@Override
			protected UpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
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
							List<Duo<? extends TRGT, ? extends TRGT>> entities = collectToList(updatableListIndex.get().keySet(), target -> new Duo<>(target, target));
							targetPersister.update(entities, false);
						});
			}
			
			class IndexedMappedAssociationUpdateContext extends UpdateContext {
				
				/** New indexes per entity */
				private final Map<TRGT, Integer> indexUpdates = new HashMap<>();
				
				public IndexedMappedAssociationUpdateContext(Duo<SRC, SRC> updatePayload) {
					super(updatePayload);
				}
				
				public Map<TRGT, Integer> getIndexUpdates() {
					return indexUpdates;
				}
			}
		};
		sourcePersister.getPersisterListener().addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
}
