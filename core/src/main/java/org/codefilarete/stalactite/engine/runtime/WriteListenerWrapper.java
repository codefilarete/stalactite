package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.EntityWriteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class for wrapping calls to {@link #insert(Iterable)} method into
 * {@link InsertListener#beforeInsert(Iterable)} and {@link InsertListener#afterInsert(Iterable)} and corresponding methods for other methods,
 * This is made through an internal {@link PersisterListenerCollection}.
 * 
 * @author Guillaume Mary
 */
public abstract class WriteListenerWrapper<C, I>
		implements InsertExecutor<C>, UpdateExecutor<C>, DeleteExecutor<C, I>, EntityWriteListener<C> {
	
	protected final PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		this.persisterListener.addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		this.persisterListener.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		this.persisterListener.addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		this.persisterListener.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		this.persisterListener.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		this.persisterListener.addDeleteByIdListener(deleteListener);
	}
	
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return this.persisterListener;
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithDeleteListener(entities, () -> this.doDelete(entities));
		}
	}
	
	abstract protected void doDelete(Iterable<? extends C> entities);
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithDeleteByIdListener(entities, () -> this.doDeleteById(entities));
		}
	}
		
	abstract protected void doDeleteById(Iterable<? extends C> entities);
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithInsertListener(entities, () -> this.doInsert(entities));
		}
	}
			
	abstract protected void doInsert(Iterable<? extends C> entities);
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithUpdateByIdListener(entities, () -> this.doUpdateById(entities));
		}
	}
	
	abstract protected void doUpdateById(Iterable<? extends C> entities);
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		if (!Iterables.isEmpty(differencesIterable)) {
			persisterListener.doWithUpdateListener(differencesIterable, allColumnsStatement, () -> this.doUpdate(differencesIterable, allColumnsStatement));
		}
	}
	
	protected abstract void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement);
}
