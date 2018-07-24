package org.gama.stalactite.persistence.id;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.ADDED;
import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.HELD;
import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.REMOVED;

/**
 * A class to compute the differences between 2 collections of {@link Identified}: addition, removal or held
 * 
 * @author Guillaume Mary
 */
public class IdentifiedCollectionDiffer {
	
	/**
	 * Computes the differences between 2 sets. Comparison between objects will be done onto Identifier payload's hashCode() + equals()
	 * 
	 * @param identifieds1 the "source" Set
	 * @param identifieds2 the modified Set
	 * @param <I> the type of Identified
	 * @param <C> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified<C>, C> Set<Diff> diffSet(Set<I> identifieds1, Set<I> identifieds2) {
		Function<I, C> surrogateAccessor = Functions.chain(Identified::getId, StatefullIdentifier::getSurrogate);
		Map<C, I> set1MappedOnIdentifier = Iterables.map(identifieds1, surrogateAccessor, Function.identity());
		Map<C, I> set2MappedOnIdentifier = Iterables.map(identifieds2, surrogateAccessor, Function.identity());
		
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
	
	<E extends Identified> Set<Integer> lookupIndexes(List<E> srcList, E searched) {
		Set<Integer> indexes = new HashSet<>();
		Iterables.consumeAll(srcList, e -> searched.getId().equals(e.getId()), (e, i) -> indexes.add(i));
		return indexes;
	}
	
	
	/**
	 * @author Guillaume Mary
	 */
	public static class AbstractDiff {
		protected final Identified sourceInstance;
		protected final Identified replacingInstance;
		private final State state;
		
		public AbstractDiff(State state, Identified sourceInstance, Identified replacingInstance) {
			this.state = state;
			this.sourceInstance = sourceInstance;
			this.replacingInstance = replacingInstance;
		}
		
		public Identified getSourceInstance() {
			return sourceInstance;
		}
		
		public Identified getReplacingInstance() {
			return replacingInstance;
		}
		
		public State getState() {
			return state;
		}
		
	}
	
	public enum State {
		/** The object has been added to the collection */
		ADDED,
		/**
		 * The object exists in both Sets, but source and replacer may differ on some fields
		 * (depending on {@link #equals(Object)} method exhaustivity)
		 */
		HELD,
		/** The object has been removed from the source object */
		REMOVED
	}
	
	/**
	 * A difference of a comparison made by {@link #diffSet(Set, Set)}
	 *
	 * On {@link State#ADDED} instances, only {@link #getReplacingInstance()} is set ({@link #getSourceInstance()} is null.
	 * On {@link State#REMOVED} instances, only {@link #getSourceInstance()} is set ({@link #getReplacingInstance()} is null.
	 * On {@link State#HELD} instances, both {@link #getSourceInstance()} and ({@link #getReplacingInstance()} are set, because entity
	 * {@link #equals(Object)} method may be implemented without taking into account all entity fields so 2 of them may differ on some fields while
	 * remaining equals, such other fields difference shall interest database to update modified columns.
	 */
	public static class Diff extends AbstractDiff {
		
		public Diff(State state, Identified sourceInstance, Identified replacingInstance) {
			super(state, sourceInstance, replacingInstance);
		}
	}
	
	public static class IndexedDiff extends AbstractDiff {
		
		private final Set<Integer> sourceIndexes;
		
		private final Set<Integer> replacerIndexes;
		
		public IndexedDiff(State state, Identified sourceInstance, Identified replacingInstance) {
			this(state, sourceInstance, replacingInstance, new HashSet<>(), new HashSet<>());
		}
		
		public IndexedDiff(State state, Identified sourceInstance, Identified replacingInstance,
						   Set<Integer> sourceIndexes, Set<Integer> replacerIndexes) {
			super(state, sourceInstance, replacingInstance);
			this.sourceIndexes = sourceIndexes;
			this.replacerIndexes = replacerIndexes;
		}
		
		public Set<Integer> getSourceIndexes() {
			return sourceIndexes;
		}
		
		public IndexedDiff addSourceIndex(int index) {
			this.sourceIndexes.add(index);
			return this;
		}
		
		public Set<Integer> getReplacerIndexes() {
			return replacerIndexes;
		}
		
		public IndexedDiff addReplacerIndex(int index) {
			this.replacerIndexes.add(index);
			return this;
		}
	}
}
