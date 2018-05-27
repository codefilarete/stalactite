package org.gama.stalactite.persistence.id.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import org.gama.lang.function.Sequence;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.mapping.IIdAccessor;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Identifier manager to be used when identifier must be fixed just before insertion.
 *
 * @author Guillaume Mary
 */
public class BeforeInsertIdentifierManager<T, I> implements IdentifierInsertionManager<T, I> {
	
	private final Class<I> identifierType;
	
	private final BeforeInsertIdentifierFixer<T, I> identifierFixer;
	
	public BeforeInsertIdentifierManager(IIdAccessor<T, I> idAccessor, Sequence<I> sequence, Class<I> identifierType) {
		this.identifierType = identifierType;
		this.identifierFixer = new BeforeInsertIdentifierFixer<>(idAccessor, sequence);
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIteratorIdAware<>(iterable, writeOperation, batchSize, identifierFixer);
	}
	
	/**
	 * Will be called for fixing entities identifier.
	 *
	 * @param <T> the entity type
	 * @param <I> the identifier type
	 */
	private static class BeforeInsertIdentifierFixer<T, I> implements Consumer<T> {
		
		private final IIdAccessor<T, I> idAccessor;
		private final Sequence<I> sequence;
		
		BeforeInsertIdentifierFixer(IIdAccessor<T, I> idAccessor, Sequence<I> sequence) {
			this.idAccessor = idAccessor;
			this.sequence = sequence;
		}
		
		@Override
		public void accept(T t) {
			idAccessor.setId(t, sequence.next());
		}
	}
	
	/**
	 * {@link JDBCBatchingIterator} aimed at fixing identifier just before insertion
	 * 
	 * @param <T> the entity type
	 */
	private static class JDBCBatchingIteratorIdAware<T> extends JDBCBatchingIterator<T> {
		
		private final Consumer<T> identifierFixer;
		
		public JDBCBatchingIteratorIdAware(Iterable<T> iterable, WriteOperation writeOperation, int batchSize, Consumer<T> identifierFixer) {
			super(iterable, writeOperation, batchSize);
			this.identifierFixer = identifierFixer;
		}
		
		/**
		 * Overriden to call identifier fixer, hence it is called before batch execution which is called by {@link #onStep()}
		 * @return same as super.next()
		 */
		@Override
		public T next() {
			T next = super.next();
			identifierFixer.accept(next);
			return next;
		}
	}
}
