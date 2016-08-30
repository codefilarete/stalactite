package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Contract of the "management" of entity identifier during insertion.
 * Kind of runtime part of {@link org.gama.stalactite.persistence.id.generator.IdentifierGenerator}
 *
 * @author Guillaume Mary
 */
interface IdentifierInsertionManager<T> {
	
	PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException;
	
	JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize);
}
