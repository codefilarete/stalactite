package org.gama.stalactite.persistence.id.diff;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
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
	 * @param identifieds1 the "source" Set
	 * @param identifieds2 the modified Set
	 * @param <I> the type of {@link Identified}
	 * @param <C> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified<C>, C> Set<Diff> diffSet(Set<I> identifieds1, Set<I> identifieds2) {
		Map<C, I> set1MappedOnIdentifier = Iterables.map(identifieds1, (Function<I, C>) (Function) SURROGATE_ACCESSOR, Function.identity());
		Map<C, I> set2MappedOnIdentifier = Iterables.map(identifieds2, (Function<I, C>) (Function) SURROGATE_ACCESSOR, Function.identity());
		
		Set<Diff> result = new HashSet<>();
		
		for (Entry<C, I> identified1 : set1MappedOnIdentifier.entrySet()) {
			Identified identified2 = set2MappedOnIdentifier.get(identified1.getKey());
			if (identified2 != null) {
				result.add(new Diff(HELD, identified1.getValue(), identified2));
			} else {
				result.add(new Diff(REMOVED, identified1.getValue(), null));
			}
		}
		set2MappedOnIdentifier.keySet().removeAll(set1MappedOnIdentifier.keySet());
		for (Entry<C, I> identifiedEntry : set2MappedOnIdentifier.entrySet()) {
			result.add(new Diff(ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
	
	/**
	 * Computes the differences between 2 lists. Comparison between objects will be done onto instance equals() method
	 *
	 * @param identifieds1 the "source" List
	 * @param identifieds2 the modified List
	 * @param <I> the type of {@link Identified}
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified> Set<IndexedDiff> diffList(List<I> identifieds1, List<I> identifieds2) {
		Set<IndexedDiff> result = new HashSet<>();
		int currentIndex = 0;
		for (I identified1 : identifieds1) {
			Set<Integer> indexes = lookupIndexes(identifieds2, identified1);
			IndexedDiff diff;
			if (indexes.isEmpty())  {
				diff = new IndexedDiff(REMOVED, identified1, null);
			} else {
				diff = new IndexedDiff(HELD, identified1, identified1, new HashSet<>(), indexes);
			}
			result.add(diff);
			diff.addSourceIndex(currentIndex++);
		}
		Set<I> added = new HashSet<>(identifieds2);
		added.removeAll(identifieds1);
		result.addAll(added.stream().map(pawn -> {
					Set<Integer> indexes = lookupIndexes(identifieds2, pawn);
					return new IndexedDiff(ADDED, null, pawn, new HashSet<>(), indexes);
				}
		).collect(Collectors.toSet()));
		
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
	<E extends Identified> Set<Integer> lookupIndexes(List<E> srcList, E searched) {
		Set<Integer> indexes = new HashSet<>();
		// comparison is done on equals()
		Iterables.consumeAll(srcList, searched::equals, (e, i) -> indexes.add(i));
		return indexes;
	}
	
	
}
