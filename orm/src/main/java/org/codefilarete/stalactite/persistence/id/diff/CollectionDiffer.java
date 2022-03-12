package org.codefilarete.stalactite.persistence.id.diff;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
import org.codefilarete.tool.trace.ModifiableInt;

import static org.codefilarete.stalactite.persistence.id.diff.State.ADDED;
import static org.codefilarete.stalactite.persistence.id.diff.State.HELD;
import static org.codefilarete.stalactite.persistence.id.diff.State.REMOVED;

/**
 * A class to compute the differences between 2 collections of objects: addition, removal or held
 *
 * @param <I> bean type
 * @author Guillaume Mary
 */
public class CollectionDiffer<I> {
	
	private final Function<I, ?> idProvider;
	
	public CollectionDiffer(Function<I, ?> idProvider) {
		this.idProvider = idProvider;
	}
	
	/**
	 * Computes the differences between 2 sets. Comparison between objects will be done onto instance equals() method
	 *
	 * @param before the "source" Set
	 * @param after the modified Set
	 * @param <C> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <C> KeepOrderSet<Diff<I>> diffSet(Set<I> before, Set<I> after) {
		Map<C, I> beforeMappedOnIdentifier = Iterables.map(before, (Function<I, C>) idProvider, Function.identity(), KeepOrderMap::new);
		Map<C, I> afterMappedOnIdentifier = Iterables.map(after, (Function<I, C>) idProvider, Function.identity(), KeepOrderMap::new);
		
		KeepOrderSet<Diff<I>> result = new KeepOrderSet<>();
		
		for (Entry<C, I> entry : beforeMappedOnIdentifier.entrySet()) {
			I afterId = afterMappedOnIdentifier.get(entry.getKey());
			if (afterId != null) {
				result.add(new Diff<>(HELD, entry.getValue(), afterId));
			} else {
				result.add(new Diff<>(REMOVED, entry.getValue(), null));
			}
		}
		afterMappedOnIdentifier.keySet().removeAll(beforeMappedOnIdentifier.keySet());
		for (Entry<C, I> identifiedEntry : afterMappedOnIdentifier.entrySet()) {
			result.add(new Diff<>(ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
	
	/**
	 * Computes the differences between 2 lists. Comparison between objects will be done onto instance equals() method
	 *
	 * @param before the "source" List
	 * @param after the modified List
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public KeepOrderSet<IndexedDiff<I>> diffList(List<I> before, List<I> after) {
		// building Map of indexes per object
		Map<I, Set<Integer>> beforeIndexes = new KeepOrderMap<>();
		Map<I, Set<Integer>> afterIndexes = new KeepOrderMap<>();
		ModifiableInt beforeIndex = new ModifiableInt(-1);	// because indexes should start at 0 as List does
		before.forEach(o -> beforeIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(beforeIndex.increment()));
		ModifiableInt afterIndex = new ModifiableInt(-1);		// because indexes should start at 0 as List does
		after.forEach(o -> afterIndexes.computeIfAbsent(o, k -> new HashSet<>()).add(afterIndex.increment()));
		
		KeepOrderSet<IndexedDiff<I>> result = new KeepOrderSet<>();
		
		// Removed instances are found with a simple minus
		Set<I> removeds = Iterables.minus(beforeIndexes.keySet(), afterIndexes.keySet());
		removeds.forEach(i -> result.add(new IndexedDiff<>(REMOVED, i, null, beforeIndexes.get(i), new HashSet<>())));
		
		// Added instances are found with a simple minus (reverse order of removed)
		Set<I> addeds = Iterables.minus(afterIndexes.keySet(), beforeIndexes.keySet(), (Function<Collection<I>, KeepOrderSet<I>>) KeepOrderSet::new);
		addeds.forEach(i -> result.add(new IndexedDiff<>(ADDED, null, i, new HashSet<>(), afterIndexes.get(i))));
		
		// There are several cases for "held" instances (those existing on both sides)
		// - if they are more instances in the new set, then those new are ADDED (with their new index)
		// - if they are less instances in the set, then the missing ones are REMOVED (with their old index)
		// - common instances are HELD (with their index)
		// This principle is applied with an Iterator of pairs of indexes : pairs contain before and after index.
		// - Pairs with a missing left or right value are declared added and removed, respectively
		// - Pairs with both values are declared held
		Set<I> helds = Iterables.intersect(afterIndexes.keySet(), beforeIndexes.keySet());
		helds.forEach(i -> {
			// NB: even if Integer can't be inherited, PairIterator is a Iterator<? extends X, ? extends X>
			Iterable<Duo<Integer, Integer>> indexPairs = () -> new UntilBothIterator<>(beforeIndexes.get(i), afterIndexes.get(i));
			// NB: These instances may no be added to result, it depends on iteration
			IndexedDiff<I> removed = new IndexedDiff<>(REMOVED, i, null);
			Object id = idProvider.apply(i);
			IndexedDiff<I> held = new IndexedDiff<>(HELD,
					// Is this can be more efficient ? shouldn't we compute a Map of i vs before/after instead of iterating on before/after for each held ?
					// ... benchmark should be done
					Iterables.find(before, e -> Predicates.equalOrNull(idProvider.apply(e), id)),
					Iterables.find(after, e -> Predicates.equalOrNull(idProvider.apply(e), id)));
			IndexedDiff<I> added = new IndexedDiff<>(ADDED, null, i);
			for (Duo<? extends Integer, ? extends Integer> indexPair : indexPairs) {
				if (indexPair.getLeft() != null && indexPair.getRight() != null) {
					held.addSourceIndex(indexPair.getLeft());
					held.addReplacerIndex(indexPair.getRight());
				} else if (indexPair.getRight() == null) {
					removed.addSourceIndex(indexPair.getLeft());
				} else if (indexPair.getLeft() == null) {    // not necessary "if" since this case is the obvious one
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
