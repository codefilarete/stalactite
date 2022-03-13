package org.codefilarete.stalactite.persistence.engine.diff;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A specialized version of {@link AbstractDiff} for indexed {@link java.util.Collection} because it keeps indexes.
 * Result of a comparison made by {@link CollectionDiffer#diffList(List, List)}.
 *
 * @author Guillaume Mary
 */
public class IndexedDiff<C> extends AbstractDiff<C> {
	
	private final Set<Integer> sourceIndexes;
	
	private final Set<Integer> replacerIndexes;
	
	/**
	 * Constructor without given index (minimal constructor).
	 * Indexes may be added later with {@link #addReplacerIndex(int)} and {@link #addSourceIndex(int)}
	 * 
	 * @param state the kind of difference
	 * @param sourceInstance initial instance
	 * @param replacingInstance replacing instance (may differ from source on attributes except id)
	 */
	public IndexedDiff(State state, C sourceInstance, C replacingInstance) {
		this(state, sourceInstance, replacingInstance, new HashSet<>(), new HashSet<>());
	}
	
	public IndexedDiff(State state, C sourceInstance, C replacingInstance,
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
	
	/**
	 * Implemented for the {@link CollectionDiffer#diffList(List, List)} method algorithm
	 * 
	 * @param o any other object
	 * @return true when source and replacing instances are equal (indexes are not taken into account)
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof IndexedDiff)) return false;
		IndexedDiff that = (IndexedDiff) o;
		return Objects.equals(getSourceInstance(), that.getSourceInstance())
				&& Objects.equals(getReplacingInstance(), that.getReplacingInstance())
 		;
	}
	
	/**
	 * @return a hash of source and replacing instances, based on same principle as {@link #equals(Object)}
	 */
	@Override
	public int hashCode() {
		return Objects.hash(getSourceInstance(), getReplacingInstance());
	}
}
