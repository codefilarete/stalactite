package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.gama.lang.Retryer;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.dml.GeneratedKeysReader;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.id.generator.AfterInsertIdentifierGenerator;
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
public class InsertExecutor<T, I> extends UpsertExecutor<T, I> {
	
	public InsertExecutor(ClassMappingStrategy<T, I> mappingStrategy, org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	protected <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<C>(statement, connectionProvider, getWriteOperationRetryer()) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				int returnGeneratedKeysFlag = getGeneratedKeysReader() == null ? Statement.NO_GENERATED_KEYS : Statement.RETURN_GENERATED_KEYS;
				// NB: simple implementation: we don't use the column-specifying signature since not all databases support reading by column name
				this.preparedStatement = connection.prepareStatement(getSQL(), returnGeneratedKeysFlag);
			}
		};
	}
	
	private GeneratedKeysReader getGeneratedKeysReader() {
		return getMappingStrategy().getGeneratedKeysReader();
	}
	
	public int insert(Iterable<T> iterable) {
		Set<Table.Column> columns = getMappingStrategy().getInsertableColumns();
		ColumnParamedSQL insertStatement = getDmlGenerator().buildInsert(columns);
		WriteOperation<Table.Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = buildJdbcBatchingIterator(iterable, writeOperation);
		
		while (jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<Table.Column, Object> insertValues = getMappingStrategy().getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	private JDBCBatchingIterator<T> buildJdbcBatchingIterator(Iterable<T> iterable, WriteOperation<Table.Column> writeOperation) {
		JDBCBatchingIterator<T> jdbcBatchingIterator;
		IdentifierGenerator identifierGenerator = getMappingStrategy().getIdentifierGenerator();
		GeneratedKeysReader generatedKeysReader = getGeneratedKeysReader();
		if (identifierGenerator instanceof AfterInsertIdentifierGenerator && generatedKeysReader != null) {
			jdbcBatchingIterator = new JDBCBatchingIteratorGeneratedKeysAware(iterable, writeOperation, getBatchSize(), generatedKeysReader,
					new AfterInsertIdentifierFixer());
		} else if (identifierGenerator instanceof BeforeInsertIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIteratorIdAware(iterable, writeOperation, getBatchSize(), new BeforeInsertIdentifierFixer());
		} else if (identifierGenerator instanceof AutoAssignedIdentifierGenerator) {
			jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
		} else {
			throw new UnsupportedOperationException("Combination of identifier generator and generated keys reader is not supported : "
					+ (identifierGenerator == null ? null : identifierGenerator.getClass().getName())
					+ " / " + (generatedKeysReader == null ? null : generatedKeysReader.getClass().getName())
			);
		}
		return jdbcBatchingIterator;
	}
	
	private class JDBCBatchingIteratorGeneratedKeysAware extends JDBCBatchingIterator<T> {
		
		private final GeneratedKeysReader generatedKeysReader;
		private final BiConsumer<T, Row> generatedKeysConsumer;
		
		/** Elements of the current step, cleared after each "onStep event" */
		private final List<T> elementsOfStep;
		
		public JDBCBatchingIteratorGeneratedKeysAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize, GeneratedKeysReader 
				generatedKeysReader, BiConsumer<T, Row> generatedKeysConsumer) {
			super(iterable, writeOperation, batchSize);
			this.elementsOfStep = new ArrayList<>(batchSize);
			this.generatedKeysReader = generatedKeysReader;
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
				List<Row> rows = generatedKeysReader.read(getWriteOperation());
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
	
	private class JDBCBatchingIteratorIdAware extends JDBCBatchingIterator<T> {
		
		private final Consumer<T> identifierFixer;
		
		public JDBCBatchingIteratorIdAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize, Consumer<T> identifierFixer) {
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
	
	private class BeforeInsertIdentifierFixer implements Consumer<T> {
		@Override
		public void accept(T t) {
			getMappingStrategy().setId(t, ((BeforeInsertIdentifierGenerator<I>) getMappingStrategy().getIdentifierGenerator()).generate());
		}
	}
	
	private class AfterInsertIdentifierFixer implements BiConsumer<T, Row> {
		@Override
		public void accept(T t, Row row) {
			I pk = (I) ((AfterInsertIdentifierGenerator) getMappingStrategy().getIdentifierGenerator()).get(row.getContent());
			getMappingStrategy().setId(t, pk);
		}
	}
}

