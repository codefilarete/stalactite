package org.codefilarete.stalactite.dsl.idpolicy;

/**
 * Contract for after-insert identifier generation policy. Since nothing needs to be configured for it, not method is added.
 *
 * @param <I> identifier type
 */
public interface GeneratedKeysPolicy<I> extends IdentifierPolicy<I> {

}
