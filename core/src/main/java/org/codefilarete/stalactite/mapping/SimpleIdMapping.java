package org.codefilarete.stalactite.mapping;

import javax.annotation.Nonnull;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Entry point for single value (hence single-column primary key), as opposed to composed, about entity identifier mapping.
 * Will mainly delegate its work to an {@link IdAccessor}, an {@link IdentifierInsertionManager} and a {@link SimpleIdentifierAssembler}
 * 
 * @author Guillaume Mary
 * @see ComposedIdMapping
 */
public class SimpleIdMapping<C, I> implements IdMapping<C, I> {
	
	private final AccessorWrapperIdAccessor<C, I> idAccessor;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private final IsNewDeterminer<C> isNewDeterminer;
	
	private final SimpleIdentifierAssembler<I, ?> identifierMarshaller;
	
	/**
	 * Main constructor
	 * 
	 * @param idAccessor entry point to get/set id of an entity
	 * @param identifierInsertionManager defines the way the id is persisted into the database
	 * @param identifierMarshaller defines the way the id is read from the database
	 */
	public SimpleIdMapping(AccessorWrapperIdAccessor<C, I> idAccessor,
						   IdentifierInsertionManager<C, I> identifierInsertionManager,
						   SimpleIdentifierAssembler<I, ?> identifierMarshaller) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
		this.identifierMarshaller = identifierMarshaller;
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			this.isNewDeterminer = new AlreadyAssignedIdDeterminer(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getIsPersistedFunction());
		} else if (identifierInsertionManager.getIdentifierType().isPrimitive()) {
			this.isNewDeterminer = new PrimitiveIdDeterminer();
		} else {
			this.isNewDeterminer = new NullableIdDeterminer();
		}
	}
	
	public SimpleIdMapping(ReversibleAccessor<C, I> identifierAccessor,
						   IdentifierInsertionManager<C, I> identifierInsertionManager,
						   SimpleIdentifierAssembler identifierMarshaller) {
		this(new AccessorWrapperIdAccessor<>(identifierAccessor), identifierInsertionManager, identifierMarshaller);
	}
	
	@Override
	public AccessorWrapperIdAccessor<C, I> getIdAccessor() {
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
	public <T extends Table<T>> SimpleIdentifierAssembler<I, T> getIdentifierAssembler() {
		return (SimpleIdentifierAssembler<I, T>) identifierMarshaller;
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
	 * For case where the identifier is already assigned : we have to delegate determination to a function
	 */
	private class AlreadyAssignedIdDeterminer implements IsNewDeterminer<C> {
		
		private final Function<C, Boolean> isPersistedFunction;
		
		private AlreadyAssignedIdDeterminer(Function<C, Boolean> isPersistedFunction) {
			this.isPersistedFunction = isPersistedFunction;
		}
		
		@Override
		public boolean isNew(C entity) {
			return !isPersistedFunction.apply(entity);
		}
	}
}
