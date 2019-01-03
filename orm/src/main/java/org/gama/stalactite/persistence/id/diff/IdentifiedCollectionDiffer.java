package org.gama.stalactite.persistence.id.diff;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.PairIterator.UntilBothIterator;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.lang.function.Functions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

import static org.gama.stalactite.persistence.id.diff.State.ADDED;
import static org.gama.stalactite.persistence.id.diff.State.HELD;
import static org.gama.stalactite.persistence.id.diff.State.REMOVED;

/**
 * A class to compute the differences between 2 collections of {@link Identified}: addition, removal or held
 *
 * @author Guillaume Mary
 */
public class IdentifiedCollectionDiffer {
	
	private static final Function<Identified, Object> SURROGATE_ACCESSOR = Functions.chain(Identified::getId, StatefullIdentifier::getSurrogate);
	
	/**
	 * Computes the differences between 2 sets. Comparison between objects will be done onto instance equals() method
	 *
	 * @param before the "source" Set
	 * @param after the modified Set
	 * @param <I> the type of {@link Identified}
	 * @param <C> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified<C>, C> Set<Diff<I>> diffSet(Set<I> before, Set<I> after) {
		Map<C, I> beforeMappedOnIdentifier = Iterables.map(before, (Function<I, C>) (Function) SURROGATE_ACCESSOR, Function.identity());
		Map<C, I> afterMappedOnIdentifier = Iterables.map(after, (Function<I, C>) (Function) SURROGATE_ACCESSOR, Function.identity());
		
		Set<Diff<I>> result = new HashSet<>();
		
		for (Entry<C, I> id : beforeMappedOnIdentifier.entrySet()) {
			I afterId = afterMappedOnIdentifier.get(id.getKey());
			if (afterId != null) {
				result.add(new Diff<>(HELD, id.getValue(), afterId));
			} else {
				result.add(new Diff<>(REMOVED, id.getValue(), null));
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
	 * @param <I> the type of {@link Identified}
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified> Set<IndexedDiff<I>> diffList(List<I> before, List<I> after) {
		// building Map of indexes per object
		Map<I, Set<Integer>> beforeIndexes = new ValueFactoryHashMap<>(k -> new HashSet<>());
		Map<I, Set<Integer>> afterIndexes = new ValueFactoryHashMap<>(k -> new HashSet<>());
		ModifiableInt beforeIndex = new ModifiableInt(-1);
		before.forEach(o -> beforeIndexes.get(o).add(beforeIndex.increment()));
		ModifiableInt afterIndex = new ModifiableInt(-1);
		after.forEach(o -> afterIndexes.get(o).add(afterIndex.increment()));
		
		Set<IndexedDiff<I>> result = new HashSet<>();
		
		// Removed instances are found with a simple minus
		Set<I> removeds = Iterables.minus(beforeIndexes.keySet(), afterIndexes.keySet());
		removeds.forEach(i -> result.add(new IndexedDiff<>(REMOVED, i, null, beforeIndexes.get(i), new HashSet<>())));
		
		// Added instances are found with a simple minus (reverse order of removed)
		Set<I> addeds = Iterables.minus(afterIndexes.keySet(), beforeIndexes.keySet());
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
			Iterable<Duo<? extends Integer, ? extends Integer>> indexPairs = () -> new UntilBothIterator<>(beforeIndexes.get(i), afterIndexes.get(i));
			// NB: These instances may no be added to result, it depends on iteration
			IndexedDiff<I> removed = new IndexedDiff<>(REMOVED, i, null);
			IndexedDiff<I> held = new IndexedDiff<>(HELD, i, i);
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
	
	/**
	 * Looks up for indexes of an object into a {@link List}. Comparison is done on equals() method.
	 * 
	 * @param srcList the source where to lookup for searched object
	 * @param searched the object to lookup
	 * @param <E> type of elements (subtype of {@link Identified})
	 * @return a Set of indexes where {@code searched} is present
	 */
	@Nonnull
	<E extends Identified> SortedSet<Integer> lookupIndexes(List<E> srcList, E searched) {
		TreeSet<Integer> indexes = new TreeSet<>();
		// comparison is done on equals()
		Iterables.consumeAll(srcList, searched::equals, (e, i) -> indexes.add(i));
		return indexes;
	}
	
	
}
