package org.codefilarete.stalactite.engine.runtime;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class for wrapping calls to {@link #insert(Iterable)} method into
 * {@link InsertListener#beforeInsert(Iterable)} and {@link InsertListener#afterInsert(Iterable)} and corresponding methods for other methods,
 * This is made through an internal {@link PersisterListenerCollection}.
 * 
 * @author Guillaume Mary
 */
public abstract class PersisterListenerWrapper<C, I> extends WriteListenerWrapper<C, I> implements PersistExecutor<C>, SelectExecutor<C, I> {
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new HashSet<>();
		} else {
			return persisterListener.doWithSelectListener(ids, () -> this.doSelect(ids));
		}
	}
	
	abstract protected Set<C> doSelect(Iterable<I> ids);
	
	/**
	 * Overridden to wrap invocations with persister listeners
	 * @param entities
	 */
	@Override
	public void persist(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) persisterListener.doWithPersistListener(entities, () -> this.doPersist(entities));
	}
	
	abstract protected void doPersist(Iterable<? extends C> entities);
	
}
