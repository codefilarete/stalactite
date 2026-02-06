package org.codefilarete.stalactite.engine.diff;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.PairIterator.UntilBothIterator;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.engine.diff.State.ADDED;
import static org.codefilarete.stalactite.engine.diff.State.HELD;
import static org.codefilarete.stalactite.engine.diff.State.REMOVED;
import static org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyWithAssociationTableEngine.INDEXED_COLLECTION_FIRST_INDEX_VALUE;

/**
 * A class to compute the differences between 2 collections of objects: addition, removal or held
 *
 * @param <C> bean type
 * @author Guillaume Mary
 */
public class CollectionDiffer<C> {
	
	private final Function<C, ?> idProvider;
	
	public CollectionDiffer(Function<C, ?> idProvider) {
		this.idProvider = idProvider;
	}
	
	/**
	 * Computes the differences between 2 sets. Comparison between objects will be done through identifier provider
	 * given at construction time
	 *
	 * @param before the "source" {@link Collection}
	 * @param after the modified {@link Collection}
	 * @param <I> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 collections, never null, empty if the 2 sets are empty. If no
	 * modification, all instances will be {@link State#HELD}.
	 */
	public <I> KeepOrderSet<Diff<C>> diff(Collection<C> before, Collection<C> after) {
		Map<I, C> beforeMappedOnIdentifier = Iterables.map(before, (Function<C, I>) idProvider, Function.identity(), KeepOrderMap::new);
		Map<I, C> afterMappedOnIdentifier = Iterables.map(after, (Function<C, I>) idProvider, Function.identity(), KeepOrderMap::new);
		
		KeepOrderSet<Diff<C>> result = new KeepOrderSet<>();
		
		for (Entry<I, C> entry : beforeMappedOnIdentifier.entrySet()) {
			C afterId = afterMappedOnIdentifier.get(entry.getKey());
			if (afterId != null) {
				result.add(new Diff<>(HELD, entry.getValue(), afterId));
			} else {
				result.add(new Diff<>(REMOVED, entry.getValue(), null));
			}
		}
		afterMappedOnIdentifier.keySet().removeAll(beforeMappedOnIdentifier.keySet());
		for (Entry<I, C> identifiedEntry : afterMappedOnIdentifier.entrySet()) {
			result.add(new Diff<>(ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
	
	/**
	 * Computes the differences between 2 collections by taking order in collections into account. Comparison between
	 * objects will be done through identifier provider given at construction time.
	 *
	 * @param before the "source" {@link Collection}
	 * @param after the modified {@link Collection}
	 * @return a set of differences between the 2 collections, never null, empty if the 2 sets are empty. If no
	 * modification, all instances will be {@link State#HELD}.
	 */
	public KeepOrderSet<IndexedDiff<C>> diffOrdered(Collection<C> before, Collection<C> after) {
		// building Map of indexes per object
		Map<C, Set<Integer>> beforeIndexes = new KeepOrderMap<>();
		Map<C, Set<Integer>> afterIndexes = new KeepOrderMap<>();
		MutableInt beforeIndex = new MutableInt(INDEXED_COLLECTION_FIRST_INDEX_VALUE - 1);	// -1 because ModifiableInt.increment(..) increments value before giving value
		before.forEach(o -> beforeIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(beforeIndex.increment()));
		MutableInt afterIndex = new MutableInt(INDEXED_COLLECTION_FIRST_INDEX_VALUE - 1);	// -1 because ModifiableInt.increment(..) increments value before giving value
		after.forEach(o -> afterIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(afterIndex.increment()));
		
		KeepOrderSet<IndexedDiff<C>> result = new KeepOrderSet<>();
		
		// Removed instances are found with a simple minus
		Set<C> removeds = Iterables.minus(beforeIndexes.keySet(), afterIndexes.keySet());
		removeds.forEach(e -> result.add(new IndexedDiff<>(REMOVED, e, null, beforeIndexes.get(e), new HashSet<>())));
		
		// Added instances are found with a simple minus (reverse order of removed)
		Set<C> addeds = Iterables.minus(afterIndexes.keySet(), beforeIndexes.keySet(), (Function<Collection<? extends C>, Set<C>>) KeepOrderSet::new);
		addeds.forEach(e -> result.add(new IndexedDiff<>(ADDED, null, e, new HashSet<>(), afterIndexes.get(e))));
		
		// There are several cases for "held" instances (those existing on both sides)
		// - if they are more instances in the new set, then those new are ADDED (with their new index)
		// - if they are fewer instances in the set, then the missing ones are REMOVED (with their old index)
		// - common instances are HELD (with their index)
		// This principle is applied with an Iterator of pairs of indexes : pairs contain before and after index.
		// - Pairs with a missing left or right value are declared added and removed, respectively
		// - Pairs with both values are declared held
		Set<C> helds = Iterables.intersect(afterIndexes.keySet(), beforeIndexes.keySet());
		helds.forEach(e -> {
			// NB: even if Integer can't be inherited, PairIterator is a Iterator<? extends X, ? extends X>
			Iterable<Duo<Integer, Integer>> indexPairs = () -> new UntilBothIterator<>(beforeIndexes.get(e), afterIndexes.get(e));
			// NB: These instances may not be added to result, it depends on iteration
			IndexedDiff<C> removed = new IndexedDiff<>(REMOVED, e, null);
			Object id = idProvider.apply(e);
			IndexedDiff<C> held = new IndexedDiff<>(HELD,
													// Is this can be more efficient ? shouldn't we compute a Map of i vs before/after instead of iterating on before/after for each held ?
													// ... benchmark should be done
													Iterables.find(before, c -> Predicates.equalOrNull(idProvider.apply(c), id)),
													Iterables.find(after, c -> Predicates.equalOrNull(idProvider.apply(c), id)));
			IndexedDiff<C> added = new IndexedDiff<>(ADDED, null, e);
			for (Duo<? extends Integer, ? extends Integer> indexPair : indexPairs) {
				if (indexPair.getLeft() != null && indexPair.getRight() != null) {
					held.addSourceIndex(indexPair.getLeft());
					held.addReplacerIndex(indexPair.getRight());
				} else if (indexPair.getRight() == null) {
					removed.addSourceIndex(indexPair.getLeft());
				} else if (indexPair.getLeft() == null) {    // unnecessary "if" since this case is the obvious one
					added.addReplacerIndex(indexPair.getRight());
				}
			}
			// adding result of iteration to final result
			if (!removed.getSourceIndexes().isEmpty()) {
				result.add(removed);
			}
			if (!held.getReplacerIndexes().isEmpty()) {
				result.add(held);
			}
			if (!added.getReplacerIndexes().isEmpty()) {
				result.add(added);
			}
		});
		
		return result;
	}
}
