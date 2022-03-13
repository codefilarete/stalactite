package org.codefilarete.stalactite.engine.diff;

/**
 * Abstract contract for a change of place in a {@link java.util.Collection}.
 *
 * @author Guillaume Mary
 * @see Diff
 * @see IndexedDiff
 */
public class AbstractDiff<C> {
	private final C sourceInstance;
	private final C replacingInstance;
	private final State state;
	
	/**
	 * Minimal constructor.
	 *
	 * @param state the kind of difference
	 * @param sourceInstance initial instance
	 * @param replacingInstance replacing instance (may differ from source on attributes except id)
	 */
	public AbstractDiff(State state, C sourceInstance, C replacingInstance) {
		this.state = state;
		this.sourceInstance = sourceInstance;
		this.replacingInstance = replacingInstance;
	}
	
	public C getSourceInstance() {
		return sourceInstance;
	}
	
	public C getReplacingInstance() {
		return replacingInstance;
	}
	
	public State getState() {
		return state;
	}
	
}
