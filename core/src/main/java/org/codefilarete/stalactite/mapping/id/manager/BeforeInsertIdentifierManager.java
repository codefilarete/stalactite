package org.codefilarete.stalactite.mapping.id.manager;

import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Identifier manager to be used when identifier must be fixed just before insertion.
 *
 * @author Guillaume Mary
 */
public class BeforeInsertIdentifierManager<T, I> implements IdentifierInsertionManager<T, I> {
	
	private static final InsertListener NOOP_INSERT_LISTENER = new InsertListener() {};
	private static final SelectListener NOOP_SELECT_LISTENER = new SelectListener() {};
	
	private final Class<I> identifierType;
	
	private final BeforeInsertIdentifierFixer<T, I> identifierFixer;
	
	public BeforeInsertIdentifierManager(IdAccessor<T, I> idAccessor, Sequence<I> sequence, Class<I> identifierType) {
		this.identifierType = identifierType;
		this.identifierFixer = new BeforeInsertIdentifierFixer<>(idAccessor, sequence);
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	@Override
	public JDBCBatchingIterator<T> buildJDBCBatchingIterator(Iterable<? extends T> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIteratorIdAware<>(entities, writeOperation, batchSize, identifierFixer);
	}
	
	@Override
	public InsertListener<T> getInsertListener() {
		return NOOP_INSERT_LISTENER;
	}
	
	@Override
	public SelectListener<T, I> getSelectListener() {
		return NOOP_SELECT_LISTENER;
	}
	
	/**
	 * Will be called for fixing entities identifier.
	 *
	 * @param <T> the entity type
	 * @param <I> the identifier type
	 */
	private static class BeforeInsertIdentifierFixer<T, I> implements Consumer<T> {
		
		private final IdAccessor<T, I> idAccessor;
		private final Sequence<I> sequence;
		
		BeforeInsertIdentifierFixer(IdAccessor<T, I> idAccessor, Sequence<I> sequence) {
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
		
		public JDBCBatchingIteratorIdAware(Iterable<? extends T> entities, WriteOperation writeOperation, int batchSize, Consumer<T> identifierFixer) {
			super(entities, writeOperation, batchSize);
			this.identifierFixer = identifierFixer;
		}
		
		/**
		 * Overridden to call identifier fixer, hence it is called before batch execution which is called by {@link #onStep()}
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
