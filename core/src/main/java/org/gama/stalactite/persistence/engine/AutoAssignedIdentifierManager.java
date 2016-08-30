package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Identifier manager during insertion for {@link org.gama.stalactite.persistence.id.generator.AutoAssignedIdentifierGenerator}.
 * As identifier is already specified on entity, nothing special must be done !
 * 
 * @author Guillaume Mary
 */
class AutoAssignedIdentifierManager<T> implements IdentifierInsertionManager<T> {
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(iterable, writeOperation, batchSize);
	}
}
