package org.codefilarete.stalactite.engine.diff;

import java.util.Set;

/**
 * A difference of a comparison made by {@link CollectionDiffer#diffSet(Set, Set)}
 *
 * On {@link State#ADDED} instances, only {@link #getReplacingInstance()} is set ({@link #getSourceInstance()} is null.
 * On {@link State#REMOVED} instances, only {@link #getSourceInstance()} is set ({@link #getReplacingInstance()} is null.
 * On {@link State#HELD} instances, both {@link #getSourceInstance()} and ({@link #getReplacingInstance()} are set, because entity
 * {@link #equals(Object)} method may be implemented without taking into account all entity fields so 2 of them may differ on some fields while
 * remaining equals, such other fields difference shall interest database to update modified columns.
 */
public class Diff<C> extends AbstractDiff<C> {
	
	/**
	 * Constructor without given index (minimal constructor).
	 * 
	 * @param state the kind of difference
	 * @param sourceInstance initial instance
	 * @param replacingInstance replacing instance (may differ from source on attributes except id)
	 */
	public Diff(State state, C sourceInstance, C replacingInstance) {
		super(state, sourceInstance, replacingInstance);
	}
}
