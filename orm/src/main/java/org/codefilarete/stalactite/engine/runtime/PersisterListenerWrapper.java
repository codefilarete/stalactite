package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;

/**
 * Class for wrapping calls to {@link EntityConfiguredJoinedTablesPersister#insert(Object)} and other update, delete, etc methods into
 * {@link InsertListener#beforeInsert(Iterable)} and {@link InsertListener#afterInsert(Iterable)} (and corresponding methods for other methods),
 * this is made through an internal {@link PersisterListenerCollection}.
 * 
 * @author Guillaume Mary
 */
public class PersisterListenerWrapper<C, I> extends PersisterWrapper<C, I> {
	
	private final PersisterListenerCollection<C, I> persisterListener = new PersisterListenerCollection<>();
	
	public PersisterListenerWrapper(EntityConfiguredJoinedTablesPersister<C, I> surrogate) {
		super(surrogate);
	}
	
	@Override
	public void addInsertListener(InsertListener insertListener) {
		this.persisterListener.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
		this.persisterListener.addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
		this.persisterListener.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
		this.persisterListener.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
		this.persisterListener.addDeleteByIdListener(deleteListener);
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return this.persisterListener;
	}
	
	@Override
	public void delete(Iterable<C> entities) {
		persisterListener.doWithDeleteListener(entities, () -> surrogate.delete(entities));
	}
	
	@Override
	public void deleteById(Iterable<C> entities) {
		persisterListener.doWithDeleteByIdListener(entities, () -> surrogate.deleteById(entities));
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		if (!Iterables.isEmpty(entities)) {
			persisterListener.doWithInsertListener(entities, () -> surrogate.insert(entities));
		}
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		if (Iterables.isEmpty(ids)) {
			return new ArrayList<>();
		} else {
			return persisterListener.doWithSelectListener(ids, () -> surrogate.select(ids));
		}
	}
	
	@Override
	public void updateById(Iterable<C> entities) {
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
