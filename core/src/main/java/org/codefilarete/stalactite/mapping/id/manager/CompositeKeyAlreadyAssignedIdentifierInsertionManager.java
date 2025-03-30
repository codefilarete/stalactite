package org.codefilarete.stalactite.mapping.id.manager;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.tool.collection.Collections;

/**
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> extends AlreadyAssignedIdentifierManager<C, I> {
	
	private final ThreadLocal<Set<C>> currentMarkedPersistedEntities;
	
	private final CurrentContextManagerInsertListener currentContextManagerInsertListener = new CurrentContextManagerInsertListener();
	
	private final CurrentContextManagerSelectListener currentContextManagerSelectListener = new CurrentContextManagerSelectListener();
	
	private final CurrentContextManagerPersistListener currentContextManagerPersistListener = new CurrentContextManagerPersistListener();
	
	public CompositeKeyAlreadyAssignedIdentifierInsertionManager(Class<I> identifierType,
																 Consumer<C> markAsPersistedFunction,
																 Function<C, Boolean> isPersistedFunction) {
		super(identifierType, markAsPersistedFunction, isPersistedFunction);
		this.currentMarkedPersistedEntities = null;
	}
	
	public CompositeKeyAlreadyAssignedIdentifierInsertionManager(Class<I> identifierType) {
		this(identifierType, new ThreadLocal<>());
	}
	
	private CompositeKeyAlreadyAssignedIdentifierInsertionManager(Class<I> identifierType, ThreadLocal<Set<C>> currentMarkedPersistedEntities) {
		super(identifierType, o -> currentMarkedPersistedEntities.get().add(o), o -> currentMarkedPersistedEntities.get().contains(o));
		this.currentMarkedPersistedEntities = currentMarkedPersistedEntities;
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
			markAsPersisted(entities);
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
		public void afterSelect(Set<? extends C> result) {
			markAsPersisted(result);
			clearCurrentEntitySet();
		}
		
		@Override
		public void onSelectError(Iterable<I> ids, RuntimeException exception) {
			clearCurrentEntitySet();
		}
	}
}
