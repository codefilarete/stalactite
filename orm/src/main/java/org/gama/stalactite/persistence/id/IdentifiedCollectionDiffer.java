package org.gama.stalactite.persistence.id;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.bean.Objects;

/**
 * A class to compute the differences between 2 collections of {@link Identified}: addition, removal or held
 * 
 * @author Guillaume Mary
 */
public class IdentifiedCollectionDiffer {
	
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
	
	/**
	 * Compute the differences between 2 sets. Comparison between objects will be done onto Identifier payload's hashCode() + equals()
	 * 
	 * @param identifieds1 the "source" Set
	 * @param identifieds2 the modified Set
	 * @param <I> the type of Identified
	 * @param <C> the type of the payload onto comparison will be done
	 * @return a set of differences between the 2 sets, never null, empty if the 2 sets are empty. If no modification, all instances will be
	 * {@link State#HELD}.
	 */
	public <I extends Identified<C>, C> Set<Diff> diffSet(Set<I> identifieds1, Set<I> identifieds2) {
		Map<C, Identified> map1 = identifieds1.stream().collect(HashMap::new, (map, i) -> map.put(i.getId().getSurrogate(), i), (a, b) -> { });
		Map<C, Identified> map2 = identifieds2.stream().collect(HashMap::new, (map, i) -> map.put(i.getId().getSurrogate(), i), (a, b) -> { });
		
		Set<Diff> result = new HashSet<>();
		
		for (Entry<C, Identified> identified1 : map1.entrySet()) {
			Identified identified2 = map2.get(identified1.getKey());
			if (identified2 != null) {
				result.add(new Diff(State.HELD, identified1.getValue(), identified2));
			} else {
				result.add(new Diff(State.REMOVED, identified1.getValue(), null));
			}
		}
		map2.keySet().removeAll(map1.keySet());
		for (Entry<C, Identified> identifiedEntry : map2.entrySet()) {
			result.add(new Diff(State.ADDED, null, identifiedEntry.getValue()));
		}
		return result;
	}
}
