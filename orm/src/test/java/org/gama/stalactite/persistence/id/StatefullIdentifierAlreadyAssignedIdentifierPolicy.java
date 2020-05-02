package org.gama.stalactite.persistence.id;

import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.ColumnOptions.AlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * Policy for entities that have a {@link StatefullIdentifier}
 * <strong>Is only supported for entities that implement {@link org.gama.stalactite.persistence.id.Identified}</strong>
 * 
 * @author Guillaume Mary
 */
public class StatefullIdentifierAlreadyAssignedIdentifierPolicy implements AlreadyAssignedIdentifierPolicy<Identified<Long>, StatefullIdentifier<Long>> {
	
	public static final AlreadyAssignedIdentifierPolicy<Identified<Long>, StatefullIdentifier<Long>> ALREADY_ASSIGNED = new StatefullIdentifierAlreadyAssignedIdentifierPolicy();
	
	@Override
	public Consumer<Identified<Long>> getMarkAsPersistedFunction() {
		return c -> c.getId().setPersisted();
	}
	
	@Override
	public Function<Identified<Long>, Boolean> getIsPersistedFunction() {
		return c -> c.getId().isPersisted();
	}
}
