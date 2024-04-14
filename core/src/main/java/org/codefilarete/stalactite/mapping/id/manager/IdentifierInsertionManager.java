package org.codefilarete.stalactite.mapping.id.manager;

import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

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
