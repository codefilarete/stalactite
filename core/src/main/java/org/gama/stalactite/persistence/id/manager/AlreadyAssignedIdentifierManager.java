package org.gama.stalactite.persistence.id.manager;

import org.gama.stalactite.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Identifier manager to be used when identifier is already specified on entity, so, nothing special must be done !
 * In production use, may be used in confonction with {@link StatefullIdentifier} as a wrapper for entity identifier.
 * 
 * @author Guillaume Mary
 * @see SimpleIdMappingStrategy#IsNewDeterminer#isNew(Object) 
 * @see StatefullIdentifier
 */
public class AlreadyAssignedIdentifierManager<C, I> implements IdentifierInsertionManager<C, I> {
	
	public static final AlreadyAssignedIdentifierManager INSTANCE = new AlreadyAssignedIdentifierManager<>(Long.class);
	
	private final Class<I> identifierType;
	
	private final SetPersistedFlagAfterInsertListener setPersistedFlagAfterInsertListener = new SetPersistedFlagAfterInsertListener();
	
	public AlreadyAssignedIdentifierManager(Class<I> identifierType) {
		this.identifierType = identifierType;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
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
		// by default nothing has to be done
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
