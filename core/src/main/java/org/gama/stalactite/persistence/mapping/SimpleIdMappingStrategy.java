package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * Entry point for single value (hence single-column primary key), as opposed to composed, about entity identifier mapping.
 * Will mainly delegate its work to an {@link IdAccessor}, an {@link IdentifierInsertionManager} and a {@link SimpleIdentifierAssembler}
 * 
 * @author Guillaume Mary
 * @see ComposedIdMappingStrategy
 */
public class SimpleIdMappingStrategy<C, I> implements IdMappingStrategy<C, I> {
	
	private final IdAccessor<C, I> idAccessor;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private final IsNewDeterminer<C> isNewDeterminer;
	
	private final SimpleIdentifierAssembler<I> identifierMarshaller;
	
	/**
	 * Main constructor
	 * 
	 * @param idAccessor entry point to get/set id of an entity
	 * @param identifierInsertionManager defines the way the id is persisted into the database
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public SimpleIdMappingStrategy(IdAccessor<C, I> idAccessor,
								   IdentifierInsertionManager<C, I> identifierInsertionManager,
								   SimpleIdentifierAssembler<I> identifierMarshaller) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
		this.identifierMarshaller = identifierMarshaller;
		if (StatefullIdentifier.class.isAssignableFrom(identifierInsertionManager.getIdentifierType())) {
			this.isNewDeterminer = new StatefullIdDeterminer();
		} else if (identifierInsertionManager.getIdentifierType().isPrimitive()) {
			this.isNewDeterminer = new PrimitiveIdDeterminer();
		} else {
			this.isNewDeterminer = new NullableIdDeterminer();
		}
	}
	
	public SimpleIdMappingStrategy(IReversibleAccessor<C, I> identifierAccessor,
								   IdentifierInsertionManager<C, I> identifierInsertionManager,
								   SimpleIdentifierAssembler identifierMarshaller) {
		this(IdAccessor.idAccessor(identifierAccessor), identifierInsertionManager, identifierMarshaller);
	}
	
	@Override
	public IdAccessor<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public IdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
	
	@Override
	public boolean isNew(@Nonnull C entity) {
		return isNewDeterminer.isNew(entity);
	}
	
	@Override
	public SimpleIdentifierAssembler<I> getIdentifierAssembler() {
		return identifierMarshaller;
	}
	
	/**
	 * Small contract to determine if an entity is persisted or not
	 * @param <T>
	 */
	private interface IsNewDeterminer<T> {
		/**
		 * @param t an entity
		 * @return true if the entity doesn't exist in database
		 */
		boolean isNew(T t);
	}
	
	/**
	 * For case where the identifier is a basic type (String, Long, ...)
	 */
	private class NullableIdDeterminer implements IsNewDeterminer<C> {
		
		@Override
		public boolean isNew(C entity) {
			return idAccessor.getId(entity) == null;
		}
	}
	
	/**
	 * For case where the identifier is a primitive type (long, int, ...)
	 */
	private class PrimitiveIdDeterminer implements IsNewDeterminer<C> {
		
		@Override
		public boolean isNew(C entity) {
			return ((Number) idAccessor.getId(entity)).intValue() == 0;
		}
	}
	
	/**
	 * For case where the identifier is a {@link StatefullIdentifier}
	 */
	private class StatefullIdDeterminer implements IsNewDeterminer<C> {
		
		@Override
		public boolean isNew(C entity) {
			return !((StatefullIdentifier) idAccessor.getId(entity)).isPersisted();
		}
	}
}
