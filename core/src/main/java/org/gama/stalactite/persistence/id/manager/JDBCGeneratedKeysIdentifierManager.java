package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.gama.lang.Duo;
import org.gama.lang.collection.PairIterator;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;
import org.gama.stalactite.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Identifier manager that gets its values from {@link PreparedStatement#getGeneratedKeys()} (available after insert SQL statement). 
 * 
 * @author Guillaume Mary
 */
public class JDBCGeneratedKeysIdentifierManager<T, I> implements IdentifierInsertionManager<T, I> {
	
	private final Class<I> identifierType;
	
	private final AfterInsertIdentifierFixer<T, I> identifierFixer;
	private final GeneratedKeysReader generatedKeysReader;
	
	public JDBCGeneratedKeysIdentifierManager(IdAccessor<T, I> idAccessor, GeneratedKeysReader<I> generatedKeysReader, Class<I> identifierType) {
		this.identifierFixer = new AfterInsertIdentifierFixer<>(idAccessor);
		this.generatedKeysReader = generatedKeysReader;
		this.identifierType = identifierType;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		// we must flag the PreparedStatement with RETURN_GENERATED_KEYS
		return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<? extends T> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIteratorGeneratedKeysAware<>(entities, writeOperation, batchSize, generatedKeysReader, identifierFixer);
	}
	
	@Override
	public InsertListener<T> getInsertListener() {
		return new InsertListener<T>() {};
	}
	
	/**
	 * Will be called for fixing entities identifier from generated keys.
	 * 
	 * @param <T> the entity type
	 * @param <I> the identifier type
	 */
	private static class AfterInsertIdentifierFixer<T, I> implements BiConsumer<T, I> {
		
		private final IdAccessor<T, I> idAccessor;
		
		private AfterInsertIdentifierFixer(IdAccessor<T, I> idAccessor) {
			this.idAccessor = idAccessor;
		}
		
		@Override
		public void accept(T t, I id) {
			idAccessor.setId(t, id);
		}
	}
	
	/**
	 * {@link JDBCBatchingIterator} aimed at reading generated keys.
	 * 
	 * @param <T> the entity type
	 */
	private static class JDBCBatchingIteratorGeneratedKeysAware<T, I> extends JDBCBatchingIterator<T> {
		
		private final GeneratedKeysReader generatedKeysReader;
		private final BiConsumer<T, I> generatedKeysConsumer;
		
		/** Elements of the current step, cleared after each "onStep event" */
		private final List<T> elementsOfStep;
		
		public JDBCBatchingIteratorGeneratedKeysAware(Iterable<? extends T> entities, WriteOperation writeOperation, int batchSize,
													  GeneratedKeysReader generatedKeysReader, BiConsumer<T, I> generatedKeysConsumer) {
			super(entities, writeOperation, batchSize);
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
				List<I> rows = generatedKeysReader.read(getWriteOperation());
				// we have a row for each entity in insertion order, so we iterate them to apply generated keys
				PairIterator<T, I> pairIterator = new PairIterator<>(elementsOfStep, rows);
				while (pairIterator.hasNext()) {
					Duo<T, I> pair = pairIterator.next();
					generatedKeysConsumer.accept(pair.getLeft(), pair.getRight());
				}
				elementsOfStep.clear();
			} catch (SQLException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
}
