package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.id.generator.AutoAssignedIdentifierGenerator;
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
public class InsertExecutor<T> extends UpsertExecutor<T> {
	
	public InsertExecutor(ClassMappingStrategy<T> mappingStrategy, org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	protected <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<C>(statement, connectionProvider, getWriteOperationRetryer()) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				this.preparedStatement = connection.prepareStatement(getSQL());
			}
		};
	}
	
	public int insert(Iterable<T> iterable) {
		Set<Table.Column> columns = getMappingStrategy().getInsertableColumns();
		ColumnParamedSQL insertStatement = getDmlGenerator().buildInsert(columns);
		WriteOperation<Table.Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = buildJdbcBatchingIterator(iterable, writeOperation);
		
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<Table.Column, Object> insertValues = getMappingStrategy().getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	private JDBCBatchingIterator<T> buildJdbcBatchingIterator(Iterable<T> iterable, WriteOperation<Table.Column> writeOperation) {
		JDBCBatchingIterator<T> jdbcBatchingIterator;
		IdentifierGenerator identifierGenerator = getMappingStrategy().getIdentifierGenerator();
		if (identifierGenerator instanceof BeforeInsertIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIteratorIdAware(iterable, writeOperation, getBatchSize(), new BeforeInsertIdentifierFixer());
		} else if (identifierGenerator instanceof AutoAssignedIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
		} else {
			throw new UnsupportedOperationException("Identifier generator is not supported : "
					+ (identifierGenerator == null ? null : identifierGenerator.getClass().getName())
			);
		}
		return jdbcBatchingIterator;
	}
	
	private class JDBCBatchingIteratorIdAware extends JDBCBatchingIterator<T> {
		
		private final Objects.Consumer<T> identifierFixer;
		
		public JDBCBatchingIteratorIdAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize, Objects.Consumer<T> identifierFixer) {
			super(iterable, writeOperation, batchSize);
			this.identifierFixer = identifierFixer;
		}
		
		@Override
		public T next() {
			T next = super.next();
			identifierFixer.accept(next);
			return next;
		}
	}
	
	private class BeforeInsertIdentifierFixer implements Objects.Consumer<T> {
		@Override
		public void accept(T t) {
			getMappingStrategy().setId(t, ((BeforeInsertIdentifierGenerator) getMappingStrategy().getIdentifierGenerator()).generate());
		}
	}
}

