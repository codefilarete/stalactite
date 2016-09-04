package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.id.generator.AlreadyAssignedIdPolicy;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Identifier manager during insertion for {@link AlreadyAssignedIdPolicy}.
 * As identifier is already specified on entity, nothing special must be done !
 * 
 * @author Guillaume Mary
 */
public class AutoAssignedIdentifierManager<T> implements IdentifierInsertionManager<T> {
	
	public static final AutoAssignedIdentifierManager INSTANCE = new AutoAssignedIdentifierManager();
	
	public AutoAssignedIdentifierManager() {
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
