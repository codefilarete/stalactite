package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.id.generator.AfterInsertIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.BeforeInsertIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.IdentifierGenerator;
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
		this.identifierInsertionManager = newIdentifierInsertionEngine();
	}
	
	private IdentifierInsertionManager<T> newIdentifierInsertionEngine() {
		IdentifierGenerator identifierGenerator = getMappingStrategy().getIdentifierGenerator();
		if (identifierGenerator instanceof AfterInsertIdentifierGenerator) {
			return new AfterInsertIdentifierManager<>(getMappingStrategy()); 
		} else if (identifierGenerator instanceof BeforeInsertIdentifierGenerator) {
			return new BeforeInsertIdentifierManager<>(getMappingStrategy());
		} else if (identifierGenerator instanceof AutoAssignedIdentifierManager) {
			return new AutoAssignedIdentifierManager<>();
		} else {
			throw new UnsupportedOperationException("Identifier generator is not supported : "
					+ (identifierGenerator == null ? null : identifierGenerator.getClass().getName())
			);
		}
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

