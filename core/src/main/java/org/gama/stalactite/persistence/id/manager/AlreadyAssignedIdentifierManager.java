package org.gama.stalactite.persistence.id.manager;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Identifier manager to be used when identifier is already specified on entity, so, nothing special must be done !
 * In production use, may be used in confonction with {@link StatefullIdentifier}
 * 
 * @author Guillaume Mary
 * @see SimpleIdMappingStrategy.IsNewDeterminer#isNew(Object) 
 * @see StatefullIdentifier
 */
public class AlreadyAssignedIdentifierManager<T, I> implements IdentifierInsertionManager<T, I> {
	
	public static final AlreadyAssignedIdentifierManager INSTANCE = new AlreadyAssignedIdentifierManager<>(Long.class);
	
	private final Class<I> identifierType;
	
	public AlreadyAssignedIdentifierManager(Class<I> identifierType) {
		this.identifierType = identifierType;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(entities, writeOperation, batchSize);
	}
}
