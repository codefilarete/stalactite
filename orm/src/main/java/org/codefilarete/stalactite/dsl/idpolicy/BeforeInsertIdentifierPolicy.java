package org.codefilarete.stalactite.dsl.idpolicy;

import org.codefilarete.tool.function.Sequence;

/**
 * Contract for before-insert identifier generation policy. Requires the {@link Sequence} that generates the id, and that will be invoked before insertion.
 *
 * @param <I> identifier type
 */
public interface BeforeInsertIdentifierPolicy<I> extends IdentifierPolicy<I> {

}
