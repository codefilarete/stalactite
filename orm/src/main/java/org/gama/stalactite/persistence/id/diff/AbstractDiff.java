package org.codefilarete.stalactite.persistence.id.diff;

/**
 * Abstract contract for a change of place in a {@link java.util.Collection}.
 *
 * @author Guillaume Mary
 * @see Diff
 * @see IndexedDiff
 */
public class AbstractDiff<I> {
	private final I sourceInstance;
	private final I replacingInstance;
	private final State state;
	
	/**
	 * Minimal constructor.
	 *
	 * @param state the kind of difference
	 * @param sourceInstance initial instance
	 * @param replacingInstance replacing instance (may differ from source on attributes except id)
	 */
	public AbstractDiff(State state, I sourceInstance, I replacingInstance) {
		this.state = state;
		this.sourceInstance = sourceInstance;
		this.replacingInstance = replacingInstance;
	}
	
	public I getSourceInstance() {
		return sourceInstance;
	}
	
	public I getReplacingInstance() {
		return replacingInstance;
	}
	
	public State getState() {
		return state;
	}
	
}
