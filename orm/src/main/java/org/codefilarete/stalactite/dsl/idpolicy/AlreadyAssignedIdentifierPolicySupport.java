package org.codefilarete.stalactite.dsl.idpolicy;

import java.util.function.Consumer;
import java.util.function.Function;

public class AlreadyAssignedIdentifierPolicySupport<C, I> implements AlreadyAssignedIdentifierPolicy<C, I> {
	
	private final Consumer<C> markAsPersistedFunction;
	
	private final Function<C, Boolean> isPersistedFunction;
	
	public AlreadyAssignedIdentifierPolicySupport(Consumer<C> markAsPersistedFunction, Function<C, Boolean> isPersistedFunction) {
		this.markAsPersistedFunction = markAsPersistedFunction;
		this.isPersistedFunction = isPersistedFunction;
	}
	
	@Override
	public Consumer<C> getMarkAsPersistedFunction() {
		return markAsPersistedFunction;
	}
	
	@Override
	public Function<C, Boolean> getIsPersistedFunction() {
		return isPersistedFunction;
	}
}
