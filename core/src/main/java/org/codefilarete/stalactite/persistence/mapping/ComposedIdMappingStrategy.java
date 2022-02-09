package org.codefilarete.stalactite.persistence.mapping;

import javax.annotation.Nonnull;

import org.codefilarete.tool.Reflections;
import org.codefilarete.stalactite.persistence.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.persistence.id.manager.IdentifierInsertionManager;

/**
 * Entry point for composed value (hence composed primary key), about entity identifier mapping.
 * Will mainly delegate its work to an {@link IdAccessor}, an {@link IdentifierInsertionManager} and a {@link ComposedIdentifierAssembler}
 * 
 * @author Guillaume Mary
 * @see SimpleIdMappingStrategy
 */
public class ComposedIdMappingStrategy<C, I> implements IdMappingStrategy<C, I> {
	
	private final IdAccessor<C, I> idAccessor;
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	private final ComposedIdentifierAssembler<I> identifierMarshaller;
	
	/**
	 * Main constructor
	 *
	 * @param idAccessor entry point to get/set id of an entity
	 * @param identifierInsertionManager defines the way the id is persisted into the database
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public ComposedIdMappingStrategy(IdAccessor<C, I> idAccessor,
									 IdentifierInsertionManager<C, I> identifierInsertionManager,
									 ComposedIdentifierAssembler<I> identifierMarshaller) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
		this.identifierMarshaller = identifierMarshaller;
	}
	
	@Override
	public IdAccessor<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public ComposedIdentifierAssembler<I> getIdentifierAssembler() {
		return identifierMarshaller;
	}
	
	@Override
	public IdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
	
	/**
	 * Will consider a new entity if its identifier is null or if all of its values are default JVM values (null or any primitive default values)
	 * @param entity any non null entity
	 * @return true if entity's id is null or all of its primitive elements are also null or default JVM values
	 */
	@Override
	public boolean isNew(@Nonnull C entity) {
		I id = idAccessor.getId(entity);
		return id == null || identifierMarshaller.getColumnValues(id).values().stream().allMatch(o -> o == null || isDefaultPrimitiveValue(o));
	}
	
	public static boolean isDefaultPrimitiveValue(Object o) {
		return Reflections.PRIMITIVE_DEFAULT_VALUES.get(o.getClass()) == o;
	} 
}
