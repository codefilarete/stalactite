package org.codefilarete.stalactite.id;

import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.ColumnOptions.AlreadyAssignedIdentifierPolicy;

/**
 * Policy for entities that have a {@link StatefulIdentifier}
 * <strong>Is only supported for entities that implement {@link Identified}</strong>
 * 
 * @author Guillaume Mary
 */
public class StatefullIdentifierAlreadyAssignedIdentifierPolicy implements AlreadyAssignedIdentifierPolicy<Identified<Long>, StatefulIdentifier<Long>> {
	
	public static final AlreadyAssignedIdentifierPolicy<Identified<Long>, StatefulIdentifier<Long>> ALREADY_ASSIGNED = new StatefullIdentifierAlreadyAssignedIdentifierPolicy();
	
	@Override
	public Consumer<Identified<Long>> getMarkAsPersistedFunction() {
		return c -> c.getId().setPersisted();
	}
	
	@Override
	public Function<Identified<Long>, Boolean> getIsPersistedFunction() {
		return c -> c.getId().isPersisted();
	}
}
