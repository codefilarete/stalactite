package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.CollectionDiffer;

/**
 * Class aimed at making the difference of entities of an {@link UpdatePayload} and updating, inserting or deleting them according to difference
 * found between the two collections.
 * Gives entry points to add some more behavior for each of those actions. 
 * 
 * @author Guillaume Mary
 */
public class CollectionUpdater<I, O, C extends Collection<O>> implements BiConsumer<Duo<I, I>, Boolean> {
	
	private final CollectionDiffer differ;
	
	private final Function<I, C> collectionGetter;
	private final BiConsumer<O, I> reverseSetter;
	private final EntityPersister<O, ?> targetPersister;
	private final boolean shouldDeleteRemoved;
	
	/**
	 * Default and simple use case constructor.
	 * See {@link #CollectionUpdater(Function, EntityPersister, BiConsumer, boolean, Function)} for particular case about id policy
	 * 
	 * @param collectionGetter getter for collection from source entity
	 * @param targetPersister target entities persister
	 * @param reverseSetter setter for applying source entity to target entities, give null if no reverse mapping exists
	 * @param shouldDeleteRemoved true to delete orphans
	 */
	public CollectionUpdater(Function<I, C> collectionGetter,
							 EntityConfiguredPersister<O, ?> targetPersister,
							 @Nullable BiConsumer<O, I> reverseSetter,
							 boolean shouldDeleteRemoved) {
		this(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved, targetPersister.getMappingStrategy()::getId);
	}
	
	/**
	 * Constructor that lets one defines id policy : in some cases default policy (based on
	 * {@link org.gama.stalactite.persistence.mapping.IdAccessor#getId(Object)}) is not sufficient, such as when {@link Collection} contains "value type".
	 * 
	 * @param collectionGetter getter for collection from source entity
	 * @param targetPersister target entities persister
	 * @param reverseSetter setter for applying source entity to target entities (used by {@link #onRemovedTarget(UpdateContext, AbstractDiff)} to 
	 * 						nullify relation), null accepted if no reverse mapping exists
	 * @param shouldDeleteRemoved true to delete orphans
	 * @param idProvider expected to provide identifier of target beans, identifier are used to store them on it (in HashMap)
	 */
	public CollectionUpdater(Function<I, C> collectionGetter,
							 EntityPersister<O, ?> targetPersister,
							 @Nullable BiConsumer<O, I> reverseSetter,
							 boolean shouldDeleteRemoved,
							 Function<O, ?> idProvider) {
		this.collectionGetter = collectionGetter;
		this.reverseSetter = reverseSetter;
		this.targetPersister = targetPersister;
		this.shouldDeleteRemoved = shouldDeleteRemoved;
		this.differ = new CollectionDiffer<>(idProvider);
	}
	
	public CollectionDiffer getDiffer() {
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
	 * @param updateContext context created by {@link #newUpdateContext(Duo)}
	 * @param allColumnsStatement indicates if all (mapped) columns of entities must be in statement, else only modified ones will be updated
	 */
	protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
		List<Duo<O, O>> updateInput = Iterables.collectToList(updateContext.getEntitiesToBeUpdated(),
				(AbstractDiff diff) -> new Duo<>((O) diff.getReplacingInstance(), (O) diff.getSourceInstance()));
		// NB: update will only be done if necessary by target persister
		targetPersister.update(updateInput, allColumnsStatement);
	}
	
	/**
	 * Deletes entities removed from collection (only when orphan removal is asked)
	 *  @param updateContext context created by {@link #newUpdateContext(Duo)}
	 * 
	 */
	protected void deleteTargets(UpdateContext updateContext) {
		targetPersister.delete(updateContext.getEntitiesToBeDeleted());
	}
	
	/**
	 * Insert entities added to collection
	 *  @param updateContext context created by {@link #newUpdateContext(Duo)}
	 * 
	 */
	protected void insertTargets(UpdateContext updateContext) {
		targetPersister.insert(updateContext.getEntitiesToBeInserted());
	}
	
	/**
	 * Method in charge of making the differences between 2 collections of entities (many side type)
	 */
	protected Set<? extends AbstractDiff<O>> diff(Collection<O> modified, Collection<O> unmodified) {
		return differ.diffSet((Set<O>) unmodified, (Set<O>) modified);
	}
	
	/**
	 * Methods asked to give a new {@link UpdateContext}. The returned instance will be passed to methods
	 * {@link #onAddedTarget(UpdateContext, AbstractDiff)}, {@link #onHeldTarget(UpdateContext, AbstractDiff)} and {@link #onRemovedTarget(UpdateContext, AbstractDiff)}.
	 * Can be overriden to return a subtype and richer {@link UpdateContext}.
	 * 
	 * @param updatePayload instance given to {@link #accept(Duo, Boolean)}
	 * @return a new {@link UpdateContext} with given payload
	 */
	protected UpdateContext newUpdateContext(Duo<I, I> updatePayload) {
		return new UpdateContext(updatePayload);
	}
	
	protected void onAddedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		// we insert only non persisted entities to prevent from a primary key conflict
		if (targetPersister.isNew(diff.getReplacingInstance())) {
			updateContext.getEntitiesToBeInserted().add(diff.getReplacingInstance());
		}
	}
	
	protected void onHeldTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		updateContext.getEntitiesToBeUpdated().add(diff);
	}
	
	protected void onRemovedTarget(UpdateContext updateContext, AbstractDiff<O> diff) {
		// we delete only persisted entity to prevent from a not found record
		if (shouldDeleteRemoved) {
			if (!targetPersister.isNew(diff.getSourceInstance())) {
				updateContext.getEntitiesToBeDeleted().add(diff.getSourceInstance());
			}
		} else // entity shouldn't be deleted, so we may have to update it
			if (reverseSetter != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				reverseSetter.accept(diff.getSourceInstance(), null);
				targetPersister.updateById(Collections.singleton(diff.getSourceInstance()));
			}
	}
	
	protected class UpdateContext {
		
		private final Duo<I, I> payload;
		/** List of many-side entities to be inserted (for massive SQL orders and better debug) */
		private final List<O> entitiesToBeInserted = new ArrayList<>();
		/** List of many-side entities to be update (for massive SQL orders and better debug) */
		private final List<AbstractDiff> entitiesToBeUpdated = new ArrayList<>();
		/** List of many-side entities to be deleted (for massive SQL orders and better debug) */
		private final List<O> entitiesToBeDeleted = new ArrayList<>();
		
		public UpdateContext(Duo<I, I> updatePayload) {
			this.payload = updatePayload;
		}
		
		public Duo<I, I> getPayload() {
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
