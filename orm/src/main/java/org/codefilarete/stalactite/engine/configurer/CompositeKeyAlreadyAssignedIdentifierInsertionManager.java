package org.codefilarete.stalactite.engine.configurer;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.collection.Collections;

/**
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> implements IdentifierInsertionManager<C, I> {
	
	private final ThreadLocal<Set<C>> currentMarkedPersistedEntities = new ThreadLocal<>();
	
	private final Consumer<C> markAsPersistedFunction = o -> currentMarkedPersistedEntities.get().add(o);
	
	private final Function<C, Boolean> isPersistedFunction = o -> currentMarkedPersistedEntities.get().contains(o);
	
	private final CurrentContextManagerInsertListener currentContextManagerInsertListener = new CurrentContextManagerInsertListener();
	
	private final CurrentContextManagerSelectListener currentContextManagerSelectListener = new CurrentContextManagerSelectListener();
	
	private final CurrentContextManagerPersistListener currentContextManagerPersistListener = new CurrentContextManagerPersistListener();
	
	private final Class<I> identifierType;
	
	public CompositeKeyAlreadyAssignedIdentifierInsertionManager(Class<I> identifierType) {
		this.identifierType = identifierType;
	}
	
	public Consumer<C> getMarkAsPersistedFunction() {
		return markAsPersistedFunction;
	}
	
	public Function<C, Boolean> getIsPersistedFunction() {
		return isPersistedFunction;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	@Override
	public JDBCBatchingIterator<C> buildJDBCBatchingIterator(Iterable<? extends C> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(entities, writeOperation, batchSize);
	}
	
	public PersistListener<C> getPersistListener() {
		return currentContextManagerPersistListener;
	}
	
	@Override
	public InsertListener<C> getInsertListener() {
		return currentContextManagerInsertListener;
	}
	
	@Override
	public SelectListener<C, I> getSelectListener() {
		return currentContextManagerSelectListener;
	}
	
	
	private void ensureCurrentEntitySet() {
		currentMarkedPersistedEntities.set(Collections.newIdentitySet(100));
	}
	
	private void clearCurrentEntitySet() {
		currentMarkedPersistedEntities.remove();
	}
	
	private class CurrentContextManagerPersistListener implements PersistListener<C> {

		@Override
		public void beforePersist(Iterable<? extends C> entities) {
			ensureCurrentEntitySet();
		}

		@Override
		public void afterPersist(Iterable<? extends C> entities) {
			clearCurrentEntitySet();
		}
	}
	
	private class CurrentContextManagerInsertListener implements InsertListener<C> {
		
		@Override
		public void beforeInsert(Iterable<? extends C> entities) {
			ensureCurrentEntitySet();
		}
		
		@Override
		public void afterInsert(Iterable<? extends C> entities) {
			clearCurrentEntitySet();
		}
		
		@Override
		public void onInsertError(Iterable<? extends C> entities, RuntimeException runtimeException) {
			clearCurrentEntitySet();
		}
	}
	
	private class CurrentContextManagerSelectListener implements SelectListener<C, I> {
		
		@Override
		public void beforeSelect(Iterable<I> ids) {
			ensureCurrentEntitySet();
		}
		
		@Override
		public void afterSelect(Iterable<? extends C> result) {
			clearCurrentEntitySet();
		}
		
		@Override
		public void onSelectError(Iterable<I> ids, RuntimeException exception) {
			clearCurrentEntitySet();
		}
	}
}
