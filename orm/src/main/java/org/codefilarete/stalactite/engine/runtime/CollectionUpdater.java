package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;
import org.codefilarete.stalactite.engine.listener.UpdateListener.UpdatePayload;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class aimed at making the difference of entities of an {@link UpdatePayload} and updating, inserting or deleting them according to difference
 * found between the two collections.
 * Gives entry points to add some more behavior for each of those actions. 
 * 
 * @param <I> source entity type 
 * @param <O> collection item type
 * @param <C> collection type
 * @author Guillaume Mary
 */
public class CollectionUpdater<I, O, C extends Collection<O>> implements BiConsumer<Duo<I, I>, Boolean> {
	
	private final CollectionDiffer<O> differ;
	
	private final Function<I, C> collectionGetter;
	private final BiConsumer<O, I> reverseSetter;
	private final EntityWriter<O> elementPersister;
	private final boolean shouldDeleteRemoved;
	
	/**
	 * Default and simple use case constructor.
	 * See {@link #CollectionUpdater(Function, EntityPersister, BiConsumer, boolean, Function)} for particular case about id policy
	 * 
	 * @param collectionGetter getter for collection from source entity
	 * @param elementPersister target entities persister
	 * @param reverseSetter setter for applying source entity to target objects, give null if no reverse mapping exists
	 * @param shouldDeleteRemoved true to delete orphans
	 */
	public CollectionUpdater(Function<I, C> collectionGetter,
							 ConfiguredPersister<O, ?> elementPersister,
							 @Nullable BiConsumer<O, I> reverseSetter,
							 boolean shouldDeleteRemoved) {
		this(collectionGetter, elementPersister, reverseSetter, shouldDeleteRemoved, elementPersister.getMapping()::getId);
	}
	
	/**
	 * Constructor that lets one defines id policy : in some cases default policy (based on
	 * {@link IdAccessor#getId(Object)}) is not sufficient, such as when {@link Collection} contains "value type".
	 * 
	 * @param collectionGetter getter for collection from source entity
	 * @param elementPersister target entities persister
	 * @param reverseSetter setter for applying source entity to target entities (used by {@link #onRemovedElements(UpdateContext, AbstractDiff)} to 
	 * 						nullify relation), null accepted if no reverse mapping exists
	 * @param shouldDeleteRemoved true to delete orphans
	 * @param idProvider expected to provide identifier of target beans, identifier are used to store them on it (in HashMap)
	 */
	public CollectionUpdater(Function<I, C> collectionGetter,
							 EntityPersister<O, ?> elementPersister,
							 @Nullable BiConsumer<O, I> reverseSetter,
							 boolean shouldDeleteRemoved,
							 Function<O, ?> idProvider) {
		this.collectionGetter = collectionGetter;
		this.reverseSetter = reverseSetter;
		this.elementPersister = new EntityPersisterEntityWriterAdaptor<>(elementPersister);
		this.shouldDeleteRemoved = shouldDeleteRemoved;
		this.differ = new CollectionDiffer<>(idProvider);
	}
	
	public CollectionUpdater(Function<I, C> collectionGetter,
							 EntityWriter<O> elementPersister,
							 @Nullable BiConsumer<O, I> reverseSetter,
							 boolean shouldDeleteRemoved,
							 Function<O, ?> idProvider) {
		this.collectionGetter = collectionGetter;
		this.reverseSetter = reverseSetter;
		this.elementPersister = elementPersister;
		this.shouldDeleteRemoved = shouldDeleteRemoved;
		this.differ = new CollectionDiffer<>(idProvider);
	}
	
	public CollectionDiffer<O> getDiffer() {
		return differ;
	}
	
	@Override
	public void accept(Duo<I, I> entry, Boolean allColumnsStatement) {
		C modified = collectionGetter.apply(entry.getLeft());
		C unmodified = collectionGetter.apply(entry.getRight());
		Set<? extends AbstractDiff<O>> diffSet = diff(modified, unmodified);
		UpdateContext updateContext = newUpdateContext(entry);
		for (AbstractDiff<O> diff : diffSet) {
			switch (diff.getState()) {
				case ADDED:
					onAddedElements(updateContext, diff);
					break;
				case HELD:
					onHeldElements(updateContext, diff);
					break;
				case REMOVED:
					onRemovedElements(updateContext, diff);
					break;
			}
		}
		// is there any better order for these statements ?
		updateTargets(updateContext, allColumnsStatement);
		deleteTargets(updateContext);
		insertTargets(updateContext);
	}
	
	/**
	 * Updates collection entities
	 * @param updateContext context created by {@link #newUpdateContext(Duo)}
	 * @param allColumnsStatement indicates if all (mapped) columns of entities must be in statement, else only modified ones will be updated
	 */
	protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
		List<Duo<O, O>> updateInput = Iterables.collectToList(updateContext.getHeldElements(),
				(AbstractDiff diff) -> new Duo<>((O) diff.getReplacingInstance(), (O) diff.getSourceInstance()));
		// NB: update will only be done if necessary by target persister
		elementPersister.update(updateInput, allColumnsStatement);
	}
	
	/**
	 * Deletes entities removed from collection (only when orphan removal is asked)
	 * @param updateContext context created by {@link #newUpdateContext(Duo)}
	 */
	protected void deleteTargets(UpdateContext updateContext) {
		elementPersister.delete(updateContext.getRemovedElements());
	}
	
	/**
	 * Insert entities added to collection
	 * @param updateContext context created by {@link #newUpdateContext(Duo)}
	 */
	protected void insertTargets(UpdateContext updateContext) {
		// added entities may be to be inserted or updated, not only inserted if they were already in another Collection
		elementPersister.persist(updateContext.getAddedElements());
	}
	
	/**
	 * Method in charge of making the differences between 2 collections of entities (many side type)
	 */
	protected Set<? extends AbstractDiff<O>> diff(Collection<O> modified, Collection<O> unmodified) {
		// Casting to Set is a bit weird here but we should not be in another since List is taken into account upstream through mapping and redirected
		// to dedicated case (ListCollectionUpdater)
		return differ.diff(unmodified, modified);
	}
	
	/**
	 * Methods asked to give a new {@link UpdateContext}. The returned instance will be passed to methods
	 * {@link #onAddedElements(UpdateContext, AbstractDiff)}, {@link #onHeldElements(UpdateContext, AbstractDiff)} and {@link #onRemovedElements(UpdateContext, AbstractDiff)}.
	 * Can be overridden to return a subtype and richer {@link UpdateContext}.
	 * 
	 * @param updatePayload instance given to {@link #accept(Duo, Boolean)}
	 * @return a new {@link UpdateContext} with given payload
	 */
	protected UpdateContext newUpdateContext(Duo<I, I> updatePayload) {
		return new UpdateContext(updatePayload);
	}
	
	protected void onAddedElements(UpdateContext updateContext, AbstractDiff<O> diff) {
		updateContext.getAddedElements().add(diff.getReplacingInstance());
	}
	
	protected void onHeldElements(UpdateContext updateContext, AbstractDiff<O> diff) {
		updateContext.getHeldElements().add(diff);
	}
	
	protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<O> diff) {
		// we delete only persisted entity to prevent from a not found record
		if (shouldDeleteRemoved) {
			if (!elementPersister.isNew(diff.getSourceInstance())) {
				updateContext.getRemovedElements().add(diff.getSourceInstance());
			}
		} else // entity shouldn't be deleted, so we may have to update it
			if (reverseSetter != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				reverseSetter.accept(diff.getSourceInstance(), null);
				elementPersister.updateById(Collections.singleton(diff.getSourceInstance()));
			}
	}
	
	protected class UpdateContext {
		
		private final Duo<I, I> payload;
		/** List of many-side entities to be inserted (for massive SQL orders and better debug) */
		private final List<O> addedElements = new ArrayList<>();
		/** List of many-side entities to be update (for massive SQL orders and better debug) */
		private final List<AbstractDiff<O>> heldElements = new ArrayList<>();
		/** List of many-side entities to be deleted (for massive SQL orders and better debug) */
		private final List<O> removedElements = new ArrayList<>();
		
		public UpdateContext(Duo<I, I> updatePayload) {
			this.payload = updatePayload;
		}
		
		public Duo<I, I> getPayload() {
			return payload;
		}
		
		public List<O> getAddedElements() {
			return addedElements;
		}
		
		public List<AbstractDiff<O>> getHeldElements() {
			return heldElements;
		}
		
		public List<O> getRemovedElements() {
			return removedElements;
		}
	}
	
	/**
	 * A dedicated interface for this class use case. Avoid to implements too many method coming from
	 * {@link EntityPersister} in particular.
	 * 
	 * @param <C> entity type to be managed
	 * @author Guillaume Mary
	 */
	public interface EntityWriter<C> {
		
		void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement);
		
		void delete(Iterable<? extends C> entities);
		
		void persist(Iterable<? extends C> entities);
		
		boolean isNew(C entity);
		
		void updateById(Iterable<? extends C> entities);
		
	}
	
	/**
	 * Internal adaptor that makes an {@link EntityPersister} become an {@link EntityWriter}.
	 * Just some methods redirecting to delegate ones.
	 * 
	 * @param <C> entity type to be managed
	 * @author Guillaume Mary
	 */
	private static class EntityPersisterEntityWriterAdaptor<C> implements EntityWriter<C> {
		
		private final EntityPersister<C, ?> delegate;
		
		private EntityPersisterEntityWriterAdaptor(EntityPersister<C, ?> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
			delegate.update(differencesIterable, allColumnsStatement);
		}
		
		@Override
		public void delete(Iterable<? extends C> entities) {
			delegate.delete(entities);
		}
		
		@Override
		public void persist(Iterable<? extends C> entities) {
			delegate.persist(entities);
		}
		
		@Override
		public boolean isNew(C entity) {
			return delegate.isNew(entity);
		}
		
		@Override
		public void updateById(Iterable<? extends C> entities) {
			delegate.updateById(entities);
		}
	}
}
