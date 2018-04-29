package org.gama.stalactite.persistence.id;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

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
				result.add(new Diff(State.HELD, identified1.getValue(), identified2));
			} else {
				result.add(new Diff(State.REMOVED, identified1.getValue(), null));
			}
		}
		set2MappedOnIdentifier.keySet().removeAll(set1MappedOnIdentifier.keySet());
		for (Entry<C, I> identifiedEntry : set2MappedOnIdentifier.entrySet()) {
			result.add(new Diff(State.ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
	
	public enum State {
		/** The object has been added to the collection */
		ADDED,
		/** The object exists but differs from the source object */
		HELD,
		/** The object has been removed from the source object */
		REMOVED
	}
	
	/**
	 * A difference element of a comparison made by {@link #diffSet(Set, Set)}
	 *
	 * On {@link State#ADDED} instances, only {@link #getReplacingInstance()} is set ({@link #getSourceInstance()} is null.
	 * On {@link State#REMOVED} instances, only {@link #getSourceInstance()} is set ({@link #getReplacingInstance()} is null.
	 * On {@link State#HELD} instances, both {@link #getSourceInstance()} and ({@link #getReplacingInstance()} are set.
	 */
	public static class Diff {
		
		private final State state;
		
		private final Identified sourceInstance;
		
		private final Identified replacingInstance;
		
		public Diff(State state, Identified sourceInstance, Identified replacingInstance) {
			this.state = state;
			this.sourceInstance = sourceInstance;
			this.replacingInstance = replacingInstance;
		}
		
		public State getState() {
			return state;
		}
		
		public Identified getSourceInstance() {
			return sourceInstance;
		}
		
		public Identified getReplacingInstance() {
			return replacingInstance;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || this.getClass() != obj.getClass()) {
				return false;
			}
			// implemented for Set
			return state.equals(((Diff) obj).state);
		}
		
		@Override
		public int hashCode() {
			// implemented for Set
			return Objects.preventNull(sourceInstance, replacingInstance).getId().hashCode();
		}
	}
}
