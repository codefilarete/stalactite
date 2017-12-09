package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Contract of the "management" of entity identifier during insertion.
 * 
 * @author Guillaume Mary
 */
public interface IdentifierInsertionManager<T, I> {
	
	/**
	 * @return the type of the identifier
	 */
	Class<I> getIdentifierType();
	
	PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException;
	
	JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize);
}
