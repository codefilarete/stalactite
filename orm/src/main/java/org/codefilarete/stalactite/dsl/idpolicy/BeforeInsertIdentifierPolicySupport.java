package org.codefilarete.stalactite.dsl.idpolicy;

import org.codefilarete.tool.function.Sequence;

/**
 * Before-insert identifier policy that will use given {@link Sequence} for all entities.
 *
 * @param <I>
 * @author Guillaume Mary
 */
public class BeforeInsertIdentifierPolicySupport<I> implements BeforeInsertIdentifierPolicy<I> {
	
	private final Sequence<I> sequence;
	
	public BeforeInsertIdentifierPolicySupport(Sequence<I> sequence) {
		this.sequence = sequence;
	}
	
	public Sequence<I> getSequence() {
		return sequence;
	}
}
