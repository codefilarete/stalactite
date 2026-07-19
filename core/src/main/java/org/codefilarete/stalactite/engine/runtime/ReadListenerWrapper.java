package org.codefilarete.stalactite.engine.runtime;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.listener.EntityReadListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class for wrapping calls to {@link #select(Iterable)} method into
 * {@link SelectListener#beforeSelect(Iterable)} and {@link SelectListener#afterSelect(Set)} and corresponding methods for other methods,
 * This is made through an internal {@link PersisterListenerCollection}.
 *
 * @author Guillaume Mary
 */
public abstract class ReadListenerWrapper<C, I> implements EntityReadExecutor<C, I>, EntityReadListener<C, I> {
	
	protected final PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new HashSet<>();
		} else {
			return persisterListener.doWithSelectListener(ids, () -> this.doSelect(ids));
		}
	}
	
	abstract protected Set<C> doSelect(Iterable<I> ids);
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		persisterListener.addSelectListener(selectListener);
	}
}
