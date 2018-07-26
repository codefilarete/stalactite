package org.gama.stalactite.persistence.id.diff;

import org.gama.stalactite.persistence.id.Identified;

/**
 * Abstract contract for a change of place in a {@link java.util.Collection}.
 *
 * @author Guillaume Mary
 * @see Diff
 * @see IndexedDiff
 */
public class AbstractDiff {
	private final Identified sourceInstance;
	private final Identified replacingInstance;
	private final State state;
	
	/**
	 * Minimal constructor.
	 *
	 * @param state the kind of difference
	 * @param sourceInstance initial instance
	 * @param replacingInstance replacing instance (may differ from source on attributes except id)
	 */
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
