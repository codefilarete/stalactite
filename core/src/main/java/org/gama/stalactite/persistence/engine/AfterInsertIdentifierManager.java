package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.gama.lang.collection.PairIterator;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.dml.GeneratedKeysReader;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.id.generator.JDBCGeneratedKeysIdPolicy;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Identifier manager during insertion for {@link JDBCGeneratedKeysIdPolicy}.
 * Identifier must be read from a ResultSet after insertion.
 *
 * @author Guillaume Mary
 */
class AfterInsertIdentifierManager<T, I> implements IdentifierInsertionManager<T> {
	
	private final AfterInsertIdentifierFixer<T, I> identifierFixer;
	private final GeneratedKeysReader generatedKeysReader;
	
	AfterInsertIdentifierManager(ClassMappingStrategy<T, I> mappingStrategy) {
		JDBCGeneratedKeysIdPolicy<I> idAssignmentPolicy = (JDBCGeneratedKeysIdPolicy<I>) mappingStrategy.getIdAssignmentPolicy();
		this.identifierFixer = new AfterInsertIdentifierFixer<>(mappingStrategy, idAssignmentPolicy);
		this.generatedKeysReader = idAssignmentPolicy.getGeneratedKeysReader();
		// protect ourselves from nonsense
		if (this.generatedKeysReader == null) {
			throw new IllegalArgumentException("Key reader should not be null");
		}
	}
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		// we must flag the PreparedStatement with RETURN_GENERATED_KEYS
		return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize) {
		return new JDBCBatchingIteratorGeneratedKeysAware<>(iterable, writeOperation, batchSize, generatedKeysReader, identifierFixer);
	}
	
	/**
	 * Will be called for fixing entities identifier from generated keys.
	 * 
	 * @param <T> the entity type
	 * @param <I> the identifier type
	 */
	private static class AfterInsertIdentifierFixer<T, I> implements BiConsumer<T, Row> {
		
		private final IEntityMappingStrategy<T, I> mappingStrategy;
		private final JDBCGeneratedKeysIdPolicy<I> idAssignmentPolicy;
		
		AfterInsertIdentifierFixer(IEntityMappingStrategy<T, I> mappingStrategy, JDBCGeneratedKeysIdPolicy<I> idAssignmentPolicy) {
			this.mappingStrategy = mappingStrategy;
			this.idAssignmentPolicy = idAssignmentPolicy;
		}
		
		@Override
		public void accept(T t, Row row) {
			I pk = idAssignmentPolicy.get(row.getContent());
			mappingStrategy.setId(t, pk);
		}
	}
	
	/**
	 * {@link JDBCBatchingIterator} aimed at reading generated keys.
	 * 
	 * @param <T> the entity type
	 */
	private static class JDBCBatchingIteratorGeneratedKeysAware<T> extends JDBCBatchingIterator<T> {
		
		private final GeneratedKeysReader generatedKeysReader;
		private final BiConsumer<T, Row> generatedKeysConsumer;
		
		/** Elements of the current step, cleared after each "onStep event" */
		private final List<T> elementsOfStep;
		
		public JDBCBatchingIteratorGeneratedKeysAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize,
													  GeneratedKeysReader generatedKeysReader, BiConsumer<T, Row> generatedKeysConsumer) {
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
}
