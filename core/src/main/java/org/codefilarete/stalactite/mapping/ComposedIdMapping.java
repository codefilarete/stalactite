package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Entry point for composed value (hence composed primary key), about entity identifier mapping.
 * Will mainly delegate its work to an {@link IdAccessor}, an {@link AlreadyAssignedIdentifierManager} and a {@link ComposedIdentifierAssembler}
 * 
 * @author Guillaume Mary
 * @see SimpleIdMapping
 */
public class ComposedIdMapping<C, I> implements IdMapping<C, I> {
	
	private final IdAccessor<C, I> idAccessor;
	private final AlreadyAssignedIdentifierManager<C, I> identifierInsertionManager;
	private final ComposedIdentifierAssembler<I, ?> identifierMarshaller;
	
	/**
	 * Main constructor
	 *
	 * @param idAccessor entry point to get/set id of an entity
	 * @param identifierInsertionManager used for {@link #isNew(Object)} computation by linking it to {@link AlreadyAssignedIdentifierManager#getIsPersistedFunction()}
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public ComposedIdMapping(IdAccessor<C, I> idAccessor,
							 AlreadyAssignedIdentifierManager<C, I> identifierInsertionManager,
							 ComposedIdentifierAssembler<I, ?> identifierMarshaller) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
		this.identifierMarshaller = identifierMarshaller;
	}
	
	/**
	 * Shortcut to {@link ComposedIdMapping#ComposedIdMapping(IdAccessor, AlreadyAssignedIdentifierManager, ComposedIdentifierAssembler)}
	 * with a {@link ReversibleAccessor} used as a property accessor.
	 *
	 * @param identifierAccessor accessor to the property identifying the entity
	 * @param identifierInsertionManager used for {@link #isNew(Object)} computation by linking it to {@link AlreadyAssignedIdentifierManager#getIsPersistedFunction()}
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public ComposedIdMapping(ReversibleAccessor<C, I> identifierAccessor,
							 AlreadyAssignedIdentifierManager<C, I> identifierInsertionManager,
							 ComposedIdentifierAssembler<I, ?> identifierMarshaller) {
		this(new AccessorWrapperIdAccessor<>(identifierAccessor), identifierInsertionManager, identifierMarshaller);
	}
	
	@Override
	public IdAccessor<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public <T extends Table<T>> IdentifierAssembler<I, T> getIdentifierAssembler() {
		return (IdentifierAssembler<I, T>) identifierMarshaller;
	}
	
	@Override
	public AlreadyAssignedIdentifierManager<C, I> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
	
	/**
	 * Will ask for persisted status to the identifier insertion manager.
	 * @param entity any entity of type C
	 * @return true if the entity is marked as not already persisted.
	 */
	@Override
	public boolean isNew(C entity) {
		return !identifierInsertionManager.getIsPersistedFunction().apply(entity);
	}
}
