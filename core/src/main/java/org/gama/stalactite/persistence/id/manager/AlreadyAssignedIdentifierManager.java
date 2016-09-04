package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Identifier manager to be used when identifier is already specified on entity, so, nothing special must be done !
 * 
 * @author Guillaume Mary
 */
public class AlreadyAssignedIdentifierManager<T> implements IdentifierInsertionManager<T> {
	
	public static final AlreadyAssignedIdentifierManager INSTANCE = new AlreadyAssignedIdentifierManager();
	
	protected AlreadyAssignedIdentifierManager() {
	}
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(iterable, writeOperation, batchSize);
	}
}
