package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.dml.GeneratedKeysReader;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.id.AfterInsertIdentifierGenerator;
import org.gama.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.id.BeforeInsertIdentifierGenerator;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dedicated class to insert statement execution
 * 
 * @author Guillaume Mary
 */
public class InsertExecutor<T> extends UpsertExecutor<T> {
	
	public InsertExecutor(ClassMappingStrategy<T> mappingStrategy, TransactionManager transactionManager, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, transactionManager, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	protected <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<C>(statement, connectionProvider, getWriteOperationRetryer()) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				if (getGeneratedKeysReader() != null) {
					// NB: simple implementation: we don't use the column-specifying signature since not all databases support reading by column name
					this.preparedStatement = connection.prepareStatement(getSQL(), Statement.RETURN_GENERATED_KEYS);
				} else {
					this.preparedStatement = connection.prepareStatement(getSQL());
				}
			}
		};
	}
	
	private GeneratedKeysReader getGeneratedKeysReader() {
		return getMappingStrategy().getGeneratedKeysReader();
	}
	
	public int insert(Iterable<T> iterable) {
		GeneratedKeysReader generatedKeysReader = getMappingStrategy().getGeneratedKeysReader();
		Set<Table.Column> columns = getMappingStrategy().getInsertableColumns();
		ColumnParamedSQL insertStatement = getDmlGenerator().buildInsert(columns);
		WriteOperation<Table.Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = buildJdbcBatchingIterator(iterable, generatedKeysReader, writeOperation);
		
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<Table.Column, Object> insertValues = getMappingStrategy().getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	private JDBCBatchingIterator<T> buildJdbcBatchingIterator(Iterable<T> iterable, GeneratedKeysReader generatedKeysReader, WriteOperation<Table.Column> writeOperation) {
		JDBCBatchingIterator<T> jdbcBatchingIterator;IdentifierGenerator identifierGenerator = getMappingStrategy().getIdentifierGenerator();
		if (identifierGenerator instanceof AfterInsertIdentifierGenerator && generatedKeysReader != null) {
			jdbcBatchingIterator = new JDBCBatchingIteratorGeneratedKeysAware(iterable, writeOperation, getBatchSize(), new AfterInsertIdentifierFixer());
		} else if (identifierGenerator instanceof BeforeInsertIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIteratorIdAware(iterable, writeOperation, getBatchSize(), new BeforeInsertIdentifierFixer());
		} else if (identifierGenerator instanceof AutoAssignedIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIterator<T>(iterable, writeOperation, getBatchSize());
		} else {
			throw new UnsupportedOperationException("Combination of identifier generator and generated keys reader is not supported : "
					+ (identifierGenerator == null ? null : identifierGenerator.getClass().getName())
					+ " / " + (generatedKeysReader == null ? null : generatedKeysReader.getClass().getName())
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
	
	private class JDBCBatchingIteratorGeneratedKeysAware extends JDBCBatchingIterator<T> {
		
		private final Objects.BiConsumer<T, Row> generatedKeysConsumer;
		
		/** Elements of the current step, cleared after each "onStep event" */
		private final List<T> elementsOfStep;
		
		public JDBCBatchingIteratorGeneratedKeysAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize, Objects.BiConsumer<T, Row> generatedKeysConsumer) {
			super(iterable, writeOperation, batchSize);
			this.elementsOfStep = new ArrayList<>(batchSize);
			this.generatedKeysConsumer = generatedKeysConsumer;
		}
		
		@Override
		public T next() {
			T next = super.next();
			elementsOfStep.add(next);
			return next;
		}
		
		@Override
		public void onStep() {
			super.onStep();
			try {
				List<Row> rows = getGeneratedKeysReader().read(getWriteOperation());
				// we have a row for each entity in insertion order, so we iterate them to apply generated keys
				PairIterator<T, Row> pairIterator = new PairIterator<>(elementsOfStep, rows);
				while (pairIterator.hasNext()) {
					Map.Entry<T, Row> pair = pairIterator.next();
					generatedKeysConsumer.accept(pair.getKey(), pair.getValue());
				}
				elementsOfStep.clear();
			} catch (SQLException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
	
	private class BeforeInsertIdentifierFixer implements Objects.Consumer<T> {
		@Override
		public void accept(T t) {
			getMappingStrategy().setId(t, ((BeforeInsertIdentifierGenerator) getMappingStrategy().getIdentifierGenerator()).generate());
		}
	}
	private class AfterInsertIdentifierFixer implements Objects.BiConsumer<T, Row> {
		@Override
		public void accept(T t, Row row) {
			Serializable pk = (Serializable) ((AfterInsertIdentifierGenerator) getMappingStrategy().getIdentifierGenerator()).get(row.getContent());
			getMappingStrategy().setId(t, pk);
		}
	}
}

