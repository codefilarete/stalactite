package org.codefilarete.stalactite.id;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.ColumnOptions.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;

/**
 * Policy for entities that have a {@link StatefulIdentifier}
 * <strong>Is only supported for entities that implement {@link Identified}</strong>
 * 
 * @author Guillaume Mary
 */
public class StatefulIdentifierAlreadyAssignedIdentifierPolicy implements AlreadyAssignedIdentifierPolicy<Identified<?>, Identifier<?>> {
	
	public static final IdentifierPolicy<Identifier<Long>> ALREADY_ASSIGNED = (IdentifierPolicy) new StatefulIdentifierAlreadyAssignedIdentifierPolicy();
	
	public static final IdentifierPolicy<Identifier<UUID>> UUID_ALREADY_ASSIGNED = (IdentifierPolicy) new StatefulIdentifierAlreadyAssignedIdentifierPolicy();
	
	@Override
	public Consumer<Identified<?>> getMarkAsPersistedFunction() {
		return c -> c.getId().setPersisted();
	}
	
	@Override
	public Function<Identified<?>, Boolean> getIsPersistedFunction() {
		return c -> c.getId().isPersisted();
	}
}
