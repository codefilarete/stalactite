package org.gama.stalactite.persistence.id.diff;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gama.stalactite.persistence.id.Identified;

/**
 * A specialized version of {@link AbstractDiff} for indexed {@link java.util.Collection} because it keeps indexes.
 * Result of a comparison made by {@link IdentifiedCollectionDiffer#diffList(List, List)}.
 *
 * @author Guillaume Mary
 */
public class IndexedDiff extends AbstractDiff {
	
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
