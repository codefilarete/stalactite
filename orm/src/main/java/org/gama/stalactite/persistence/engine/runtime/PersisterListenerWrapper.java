package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;

/**
 * Class for wrapping calls to {@link IEntityConfiguredJoinedTablesPersister#insert(Object)} and other update, delete, etc methods into
 * {@link InsertListener#beforeInsert(Iterable)} and {@link InsertListener#afterInsert(Iterable)} (and corresponding methods for other methods),
 * this is made throught an internal {@link PersisterListener}.
 * 
 * @author Guillaume Mary
 */
public class PersisterListenerWrapper<C, I> extends PersisterWrapper<C, I> {
	
	private final PersisterListener<C, I> persisterListener = new PersisterListener<>();
	
	public PersisterListenerWrapper(IEntityConfiguredJoinedTablesPersister<C, I> surrogate) {
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
	public PersisterListener<C, I> getPersisterListener() {
		return this.persisterListener;
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		}
		return persisterListener.doWithDeleteListener(entities, () -> surrogate.delete(entities));
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		}
		return persisterListener.doWithDeleteByIdListener(entities, () -> surrogate.deleteById(entities));
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		if (Iterables.isEmpty(entities)) {
			return 0;
		} else {
			return persisterListener.doWithInsertListener(entities, () -> surrogate.insert(entities));
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
	public int updateById(Iterable<C> entities) {
		if (Iterables.isEmpty(entities)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return persisterListener.doWithUpdateByIdListener(entities, () -> surrogate.updateById(entities));
		}
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		if (Iterables.isEmpty(differencesIterable)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return persisterListener.doWithUpdateListener(differencesIterable, allColumnsStatement, surrogate::update);
		}
	}
}
