package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class for wrapping calls to {@link ConfiguredRelationalPersister#insert(Object)} and other update, delete, etc methods into
 * {@link InsertListener#beforeInsert(Iterable)} and {@link InsertListener#afterInsert(Iterable)} (and corresponding methods for other methods),
 * this is made through an internal {@link PersisterListenerCollection}.
 * 
 * @author Guillaume Mary
 */
public class PersisterListenerWrapper<C, I> extends PersisterWrapper<C, I> {
	
	private final PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	
	public PersisterListenerWrapper(ConfiguredRelationalPersister<C, I> surrogate) {
		super(surrogate);
	}
	
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
		this.surrogate.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		this.persisterListener.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		this.persisterListener.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		this.persisterListener.addDeleteByIdListener(deleteListener);
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return this.persisterListener;
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		persisterListener.doWithDeleteListener(entities, () -> surrogate.delete(entities));
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		persisterListener.doWithDeleteByIdListener(entities, () -> surrogate.deleteById(entities));
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithInsertListener(entities, () -> surrogate.insert(entities));
		}
	}
	
	/**
	 * Overriden to wrap invokations with persister listeners
	 * @param entities
	 */
	@Override
	public void persist(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithPersistListener(entities, () -> {
				// we redirect all invokations to ourselves because targetted methods invoke their listeners
				PersistExecutor.persist(entities, this::isNew, this, this, this, this::getId);
			});
		}
	}

	@Override
	public Set<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new HashSet<>();
		} else {
			return persisterListener.doWithSelectListener(ids, () -> surrogate.select(ids));
		}
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithUpdateByIdListener(entities, () -> surrogate.updateById(entities));
		}
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		if (!Iterables.isEmpty(differencesIterable)) {
			persisterListener.doWithUpdateListener(differencesIterable, allColumnsStatement, (BiConsumer<Iterable<? extends Duo<C, C>>, Boolean>) surrogate::update);
		}
	}
}
