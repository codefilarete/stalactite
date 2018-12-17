package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IdentifiedCollectionDiffer;

/**
 * Class aimed at making the difference of entities of an {@link UpdatePayload} and updating, inserting or deleting them according to difference
 * found between the two collections.
 * Gives entry points to add some more behavior for each of those actions. 
 * 
 * @author Guillaume Mary
 */
public class CollectionUpdater<I extends Identified, O extends Identified, C extends Collection<O>>
		implements BiConsumer<UpdatePayload<? extends I, ?>, Boolean> {
	
	protected final IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	private final Function<I, C> collectionGetter;
	private final BiConsumer<O, I> reverseSetter;
	private final Persister<O, ?, ?> targetPersister;
	private final boolean shouldDeleteRemoved;
	
	/**
	 * @param collectionGetter getter for collection from source entity
	 * @param targetPersister target entities persister
	 * @param reverseSetter setter for applying source entity to target entities, give null if no reverse mapping exists
	 * @param shouldDeleteRemoved true to delete orphans
	 */
	CollectionUpdater(Function<I, C> collectionGetter, Persister<O, ?, ?> targetPersister, @Nullable BiConsumer<O, I> reverseSetter, boolean shouldDeleteRemoved) {
		this.collectionGetter = collectionGetter;
		this.reverseSetter = reverseSetter;
		this.targetPersister = targetPersister;
		this.shouldDeleteRemoved = shouldDeleteRemoved;
	}
	
	@Override
	public void accept(UpdatePayload<? extends I, ?> entry, Boolean allColumnsStatement) {
		C modified = collectionGetter.apply(entry.getEntities().getLeft());
		C unmodified = collectionGetter.apply(entry.getEntities().getRight());
		Set<? extends AbstractDiff> diffSet = diff(modified, unmodified);
		UpdateContext updateContext = newUpdateContext(entry);
		for (AbstractDiff diff : diffSet) {
			switch (diff.getState()) {
				case ADDED:
					onAddedTarget(updateContext, diff);
					break;
				case HELD:
					onHeldTarget(updateContext, diff);
					break;
				case REMOVED:
					onRemovedTarget(updateContext, diff);
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
	 *  @param updateContext context created by {@link #newUpdateContext(UpdatePayload)}
	 * @param allColumnsStatement indicates if all (mapped) columns of entities must be in statement, else only modified ones will be updated
	 */
	protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
		for (AbstractDiff diff : updateContext.getEntitiesToBeUpdated()) {
			// NB: update will only be done if necessary by target persister
			targetPersister.update((O) diff.getReplacingInstance(), (O) diff.getSourceInstance(), allColumnsStatement);
		}
	}
	
	/**
	 * Deletes entities removed from collection (only when orphan removal is asked)
	 *  @param updateContext context created by {@link #newUpdateContext(UpdatePayload)}
	 * 
	 */
	protected void deleteTargets(UpdateContext updateContext) {
		targetPersister.delete(updateContext.getEntitiesToBeDeleted());
	}
	
	/**
	 * Insert entities added to collection
	 *  @param updateContext context created by {@link #newUpdateContext(UpdatePayload)}
	 * 
	 */
	protected void insertTargets(UpdateContext updateContext) {
		targetPersister.insert(updateContext.getEntitiesToBeInserted());
	}
	
	/**
	 * Method in charge of making the differences between 2 collections of entities (many side type)
	 */
	protected Set<? extends AbstractDiff> diff(Collection<O> modified, Collection<O> unmodified) {
		return differ.diffSet((Set) unmodified, (Set) modified);
	}
	
	/**
	 * Methods asked to give a new {@link UpdateContext}, this instance will be passed to entry point methods of Insert, Update and Delete actions.
	 * Can be overriden to return a subtype and richer {@link UpdateContext}.
	 * 
	 * @param updatePayload instance given to {@link #accept(UpdatePayload, Boolean)}
	 * @return a new {@link UpdateContext} with given payload
	 */
	protected UpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
		return new UpdateContext(updatePayload);
	}
	
	protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		// we insert only non persisted entities to prevent from a primary key conflict
		if (Identified.NON_PERSISTED_PREDICATE.test(diff.getReplacingInstance())) {
			updateContext.getEntitiesToBeInserted().add((O) diff.getReplacingInstance());
		}
	}
	
	protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		updateContext.getEntitiesToBeUpdated().add(diff);
	}
	
	protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		// we delete only persisted entity to prevent from a not found record
		if (shouldDeleteRemoved) {
			if (Identified.PERSISTED_PREDICATE.test(diff.getSourceInstance())) {
				updateContext.getEntitiesToBeDeleted().add(diff.getSourceInstance());
			}
		} else // entity shouldn't be deleted, so we may have to update it
			if (reverseSetter != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				reverseSetter.accept((O) diff.getSourceInstance(), null);
				targetPersister.updateById((O) diff.getSourceInstance());
			}
	}
	
	protected class UpdateContext {
		
		private final UpdatePayload<? extends I, ?> payload;
		/** List of many-side entities to be inserted (for massive SQL orders and better debug) */
		private final List<O> entitiesToBeInserted = new ArrayList<>();
		/** List of many-side entities to be update (for massive SQL orders and better debug) */
		private final List<AbstractDiff> entitiesToBeUpdated = new ArrayList<>();
		/** List of many-side entities to be deleted (for massive SQL orders and better debug) */
		private final List<O> entitiesToBeDeleted = new ArrayList<>();
		
		public UpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
			this.payload = updatePayload;
		}
		
		public UpdatePayload<? extends I, ?> getPayload() {
			return payload;
		}
		
		public List<O> getEntitiesToBeInserted() {
			return entitiesToBeInserted;
		}
		
		public List<AbstractDiff> getEntitiesToBeUpdated() {
			return entitiesToBeUpdated;
		}
		
		public List<O> getEntitiesToBeDeleted() {
			return entitiesToBeDeleted;
		}
	}
	
}
