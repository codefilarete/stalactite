package org.codefilarete.stalactite.dsl.idpolicy;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Contract for already-assigned identifier policy. Requires the methods that store entity persistence state.
 *
 * @param <I> identifier type
 */
public interface AlreadyAssignedIdentifierPolicy<C, I> extends IdentifierPolicy<I> {
	
	Consumer<C> getMarkAsPersistedFunction();
	
	Function<C, Boolean> getIsPersistedFunction();
}
