package org.gama.stalactite.persistence.engine.runtime;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.IMutator;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.structure.Column;

import static org.gama.lang.collection.Iterables.collectToList;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithIndexedMappedAssociationEngine<I extends Identified, O extends Identified, J extends Identifier, C extends List<O>>
		extends OneToManyWithMappedAssociationEngine<I, O, J, C> {
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<O, Integer>> updatableListIndex = new ThreadLocal<>();
	private final Column indexingColumn;
	private final SerializableFunction<O, I> reverseGetter;
	
	public OneToManyWithIndexedMappedAssociationEngine(PersisterListener<I, J> persisterListener,
													   Persister<O, J, ?> targetPersister,
													   Function<I, C> collectionGetter,
													   BiConsumer<O, I> reverseSetter,
													   JoinedTablesPersister<I, J, ?> joinedTablesPersister,
													   Column indexingColumn,
													   SerializableFunction<O, I> reverseGetter) {
		super(persisterListener, targetPersister, collectionGetter, reverseSetter, joinedTablesPersister);
		this.indexingColumn = indexingColumn;
		this.reverseGetter = reverseGetter;
		
	}
	
	@Override
	protected BeanRelationFixer newRelationFixer(CascadeMany<I, O, J, C> cascadeMany, IMutator<I, C> collectionSetter, SerializableBiConsumer<O, I> reverseMember,
												 PersisterListener<I, J> persisterListener) {
		CascadeManyList<I, O, J, C> refinedCascadeMany = (CascadeManyList<I, O, J, C>) cascadeMany;
		return addIndexSelection(
				joinedTablesPersister,
				refinedCascadeMany.getCollectionTargetClass(),
				persisterListener,
				collectionSetter, reverseMember);
	}
	
	private BeanRelationFixer addIndexSelection(JoinedTablesPersister<I, J, ?> joinedTablesPersister,
												Class<C> collectionTargetClass,
												PersisterListener<I, J> persisterListener,
												IMutator<I, C> collectionSetter,
												SerializableBiConsumer<O, I> reverseMember) {
		targetPersister.getSelectExecutor().getMappingStrategy().addSilentColumnSelecter(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
		// May be changed if any performance issue is noticed
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				updatableListIndex.set(new HashMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends I> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(i -> {
						List<O> apply = collectionGetter.apply(i);
						apply.sort(Comparator.comparingInt(o -> updatableListIndex.get().get(o)));
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<J> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				updatableListIndex.remove();
			}
		});
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
			Map<O, Integer> indexPerBean = updatableListIndex.get();
			// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
			// because it was read from ResultSet
			// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
			Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
			indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
		});
		return BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				collectionTargetClass, reverseMember);
	}
	
	@Override
	public void addInsertCascade() {
		// For a List and a given manner to get its owner (so we can deduce index value), we configure persistence to keep index value in database
		addIndexInsertion();
//		addIndexInsertion((CascadeManyList<I, O, J, ? extends List<O>>) cascadeMany);
		super.addInsertCascade();
	}
	
	/**
	 * Adds a "listener" that will amend insertion of the index column filled with its value
	 */
	private void addIndexInsertion() {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(indexingColumn,
				(Function<O, Object>) target -> {
					I source = reverseGetter.apply(target);
					if (source == null) {
						MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
						Method method = methodReferenceCapturer.findMethod(reverseGetter);
						throw new IllegalStateException("Can't get index : " + target + " is not associated with a " + Reflections.toString(method.getReturnType()) + " : "
								+ Reflections.toString(method) + " returned null");
					}
					List<O> collection = collectionGetter.apply(source);
					return collection.indexOf(target);
				});
	}
	
	@Override
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		
		targetPersister.getMappingStrategy().addSilentColumnUpdater(indexingColumn,
				// Thread safe by updatableListIndex access
				(Function<O, Object>) o -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(o)).get());
		
		BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener = new CollectionUpdater<I, O, C>(collectionGetter, targetPersister,
				reverseSetter, shouldDeleteRemoved) {
			
			@Override
			protected Set<? extends AbstractDiff> diff(Collection<O> modified, Collection<O> unmodified) {
				return differ.diffList((List) unmodified, (List) modified);
			}
			
			@Override
			protected UpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
				return new IndexedMappedAssociationUpdateContext(updatePayload);
			}
			
			@Override
			protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
				super.onHeldTarget(updateContext, diff);
				Set<Integer> minus = Iterables.minus(((IndexedDiff) diff).getReplacerIndexes(), ((IndexedDiff) diff).getSourceIndexes());
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
							List<Duo<? extends O, ? extends O>> entities = collectToList(updatableListIndex.get().keySet(), o -> new Duo<>(o, o));
							targetPersister.update(entities, false);
						});
			}
			
			class IndexedMappedAssociationUpdateContext extends UpdateContext {
				
				/** New indexes per entity */
				private final Map<O, Integer> indexUpdates = new HashMap<>();
				
				public IndexedMappedAssociationUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
					super(updatePayload);
				}
				
				public Map<O, Integer> getIndexUpdates() {
					return indexUpdates;
				}
			}
		};
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
}
