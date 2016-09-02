package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.id.generator.BeforeInsertIdPolicy;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Identifier manager during insertion for {@link BeforeInsertIdPolicy}.
 * Identifier must be fixed before insertion, so {@link BeforeInsertIdPolicy} must be called also before.
 *
 * @author Guillaume Mary
 */
class BeforeInsertIdentifierManager<T, I> implements IdentifierInsertionManager<T> {
	
	private final BeforeInsertIdentifierFixer<T, I> identifierFixer;
	
	BeforeInsertIdentifierManager(ClassMappingStrategy<T, I> mappingStrategy) {
		this.identifierFixer = new BeforeInsertIdentifierFixer<>(mappingStrategy, (BeforeInsertIdPolicy<I>) mappingStrategy.getIdAssignmentPolicy());
	}
	
	@Override
	public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
		return connection.prepareStatement(sql);
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<T> iterable, WriteOperation<Column> writeOperation, int batchSize) {
		return new JDBCBatchingIteratorIdAware<>(iterable, writeOperation, batchSize, identifierFixer);
	}
	
	/**
	 * Will be called for fixing entities identifier.
	 *
	 * @param <T> the entity type
	 * @param <I> the identifier type
	 */
	private static class BeforeInsertIdentifierFixer<T, I> implements Consumer<T> {
		
		private final IEntityMappingStrategy<T, I> mappingStrategy;
		private final BeforeInsertIdPolicy<I> idAssignmentPolicy;
		
		BeforeInsertIdentifierFixer(IEntityMappingStrategy<T, I> mappingStrategy, BeforeInsertIdPolicy<I> idAssignmentPolicy) {
			this.mappingStrategy = mappingStrategy;
			this.idAssignmentPolicy = idAssignmentPolicy;
		}
		
		@Override
		public void accept(T t) {
			mappingStrategy.setId(t, idAssignmentPolicy.generate());
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
