package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Dedicated class to insert statement execution
 *
 * @author Guillaume Mary
 */
public class InsertExecutor<T, I> extends UpsertExecutor<T, I> {
	
	private final IdentifierInsertionManager<T> identifierInsertionManager;
	
	public InsertExecutor(ClassMappingStrategy<T, I> mappingStrategy, org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
		this.identifierInsertionManager = mappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager();
	}
	
	protected <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<C>(statement, connectionProvider, getWriteOperationRetryer()) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				// NB: simple implementation: we don't use the column-specifying signature since not all databases support reading by column name
				this.preparedStatement = identifierInsertionManager.prepareStatement(connection, getSQL());
			}
		};
	}
	
	public int insert(Iterable<T> iterable) {
		Set<Table.Column> columns = getMappingStrategy().getInsertableColumns();
		ColumnParamedSQL insertStatement = getDmlGenerator().buildInsert(columns);
		WriteOperation<Table.Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = identifierInsertionManager.buildJDBCBatchingIterator(iterable, writeOperation, getBatchSize());
		
		while (jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<Table.Column, Object> insertValues = getMappingStrategy().getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
}

