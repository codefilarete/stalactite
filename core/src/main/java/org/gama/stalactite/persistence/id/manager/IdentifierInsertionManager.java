package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for entity identifier "management" at insertion time.
 * 
 * @author Guillaume Mary
 */
public interface IdentifierInsertionManager<C, I> {
	
	/**
	 * @return the type of the identifier
	 */
	Class<I> getIdentifierType();
	
	/**
	 * Delegation of statement creation for insertion of entities, because some managers may need to ask JDBC for generated keys retrieval.
	 * This implementation will call {@link Connection#prepareStatement(String)}
	 * 
	 * @param connection a JDBC connection
	 * @param sql the sql to be prepared
	 * @return a new {@link PreparedStatement} for the given sql
	 * @throws SQLException if statement creation fails
	 */
	default PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	/**
	 * Delegation of {@link JDBCBatchingIterator} creation, because some implementations may read generated keys or fill operation
	 * with "just generated id"
	 * 
	 * @param entities entities to be inserted
	 * @param writeOperation the underlying helper to be called for sql order execution
	 * @param batchSize batch size to apply to the returned {@link JDBCBatchingIterator}
	 * @return a new {@link JDBCBatchingIterator} 
	 */
	JDBCBatchingIterator<C> buildJDBCBatchingIterator(Iterable<? extends C> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize);
	
	InsertListener<C> getInsertListener();
	
	SelectListener<C,I> getSelectListener();
}
