package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Identifier manager to be used when identifier is already specified on entity, so, nothing special must be done !
 * In production use, may be used in confonction with {@link StatefullIdentifier}
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.persistence.mapping.IdMappingStrategy.IsNewDeterminer#isNew(Object) 
 * @see org.gama.stalactite.persistence.mapping.IdMappingStrategy.WrappedIdIsNewDeterminer
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
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(iterable, writeOperation, batchSize);
	}
}
