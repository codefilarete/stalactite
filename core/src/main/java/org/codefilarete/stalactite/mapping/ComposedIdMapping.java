package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;

/**
 * Entry point for composed value (hence composed primary key), about entity identifier mapping.
 * Will mainly delegate its work to an {@link IdAccessor}, an {@link IdentifierInsertionManager} and a {@link ComposedIdentifierAssembler}
 * 
 * @author Guillaume Mary
 * @see SimpleIdMapping
 */
public class ComposedIdMapping<C, I> implements IdMapping<C, I> {
	
	private final IdAccessor<C, I> idAccessor;
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	private final ComposedIdentifierAssembler<I, ?> identifierMarshaller;
	
	/**
	 * Main constructor
	 *
	 * @param idAccessor entry point to get/set id of an entity
	 * @param identifierInsertionManager defines the way the id is persisted into the database
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public ComposedIdMapping(IdAccessor<C, I> idAccessor,
							 IdentifierInsertionManager<C, I> identifierInsertionManager,
							 ComposedIdentifierAssembler<I, ?> identifierMarshaller) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
		this.identifierMarshaller = identifierMarshaller;
	}
	
	/**
	 * Shortcut to {@link ComposedIdMapping#ComposedIdMapping(IdAccessor, IdentifierInsertionManager, ComposedIdentifierAssembler)}
	 * with a {@link ReversibleAccessor} used as a property accessor.
	 *
	 * @param identifierAccessor accessor to the property identifying the entity
	 * @param identifierInsertionManager defines the way the id is persisted into the database
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public ComposedIdMapping(ReversibleAccessor<C, I> identifierAccessor,
							 IdentifierInsertionManager<C, I> identifierInsertionManager,
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
	public IdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
	
	/**
	 * Will consider a new entity if its identifier is null or if all of its values are default JVM values (null or any primitive default values)
	 * @param entity any entity of type C
	 * @return true if entity's id is null or all of its primitive elements are also null or default JVM values
	 */
	@Override
	public boolean isNew(C entity) {
		I id = idAccessor.getId(entity);
		return id == null || identifierMarshaller.getColumnValues(id).values().stream().allMatch(o -> o == null || isDefaultPrimitiveValue(o));
	}
	
	public static boolean isDefaultPrimitiveValue(Object o) {
		return Reflections.PRIMITIVE_DEFAULT_VALUES.get(o.getClass()) == o;
	} 
}
