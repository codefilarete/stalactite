package org.gama.stalactite.persistence.id.manager;

import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * Identifier manager to be used when identifier is already specified on entity, therefore this requires :
 * <ul>
 * <li>a way to know if entity is already present in database, this is left to a {@link Function}&lt;C, Boolean&gt;</li>
 * <li>the entity to be marked as persisted in any kind of way, this is left to a {@link Consumer}</li>
 * </ul>
 * 
 * A way o managing it can be to create a wrapper around identifier.
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy.IsNewDeterminer#isNew(Object) 
 */
public class AlreadyAssignedIdentifierManager<C, I> implements IdentifierInsertionManager<C, I> {
	
	private final Class<I> identifierType;
	
	private final SetPersistedFlagAfterInsertListener setPersistedFlagAfterInsertListener = new SetPersistedFlagAfterInsertListener();
	
	/** The {@link Consumer} that allows instances to be marked as persisted */
	private final Consumer<C> markAsPersistedFunction;
	
	/** The {@link Function} that allows to know if an instance is already persisted (expected to exist in database) */
	private final Function<C, Boolean> isPersistedFunction;
	
	public AlreadyAssignedIdentifierManager(Class<I> identifierType,
											Consumer<C> markAsPersistedFunction,
											Function<C, Boolean> isPersistedFunction) {
		this.identifierType = identifierType;
		this.markAsPersistedFunction = markAsPersistedFunction;
		this.isPersistedFunction = isPersistedFunction;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	public Function<C, Boolean> getIsPersistedFunction() {
		return isPersistedFunction;
	}
	
	@Override
	public JDBCBatchingIterator<C> buildJDBCBatchingIterator(Iterable<? extends C> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(entities, writeOperation, batchSize);
	}
	
	@Override
	public InsertListener<C> getInsertListener() {
		return this.setPersistedFlagAfterInsertListener;
	}
	
	public void setPersistedFlag(C e) {
		if (markAsPersistedFunction != null ) {
			markAsPersistedFunction.accept(e);
		}
	}
	
	public class SetPersistedFlagAfterInsertListener implements InsertListener<C> {
		
		@Override
		public void afterInsert(Iterable<? extends C> entities) {
			for (C e : entities) {
				setPersistedFlag(e);
			}
		}
	
	}
	
	
}
