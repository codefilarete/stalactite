package org.gama.stalactite.persistence.engine.listening;

import org.gama.lang.bean.ISilentDelegate;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;

/**
 * Simple class that centralize persistence event listening. Delegates listening to encapsulated instance.
 * 
 * @author Guillaume Mary
 */
public class PersisterListener<T> {
	
	private InsertListenerCollection<T> insertListener = new InsertListenerCollection<>();
	private UpdateRouglyListenerCollection<T> updateRouglyListener = new UpdateRouglyListenerCollection<>();
	private UpdateListenerCollection<T> updateListener = new UpdateListenerCollection<>();
	private DeleteListenerCollection<T> deleteListener = new DeleteListenerCollection<>();
	private SelectListenerCollection<T> selectListener = new SelectListenerCollection<>();
	
	public InsertListenerCollection<T> getInsertListener() {
		return insertListener;
	}
	
	public PersisterListener<T> addInsertListener(IInsertListener<T> insertListener) {
		this.insertListener.add(insertListener);
		return this;
	}
	
	public <R> R doWithInsertListener(Iterable<T> iterable, ISilentDelegate<R> delegate) {
		insertListener.beforeInsert(iterable);
		R result = delegate.execute();
		insertListener.afterInsert(iterable);
		return result;
	}
	
	public UpdateRouglyListenerCollection<T> getUpdateRouglyListener() {
		return updateRouglyListener;
	}
	
	public PersisterListener<T> addUpdateRouglyListener(IUpdateRouglyListener<T> updateRouglyListener) {
		this.updateRouglyListener.add(updateRouglyListener);
		return this;
	}
	
	public <R> R doWithUpdateRouglyListener(Iterable<T> iterable, ISilentDelegate<R> delegate) {
		updateRouglyListener.beforeUpdateRoughly(iterable);
		R result = delegate.execute();
		updateRouglyListener.afterUpdateRoughly(iterable);
		return result;
	}
	
	public UpdateListenerCollection<T> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<T> addUpdateListener(IUpdateListener<T> updateListener) {
		this.updateListener.add(updateListener);
		return this;
	}
	
	public <R> R doWithUpdateListener(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement, ISilentDelegate<R> delegate) {
		updateListener.beforeUpdate(differencesIterable, allColumnsStatement);
		R result = delegate.execute();
		updateListener.afterUpdate(differencesIterable, allColumnsStatement);
		return result;
	}
	
	public DeleteListenerCollection<T> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<T> addDeleteListener(IDeleteListener<T> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public <R> R doWithDeleteListener(Iterable<T> iterable, ISilentDelegate<R> delegate) {
		deleteListener.beforeDelete(iterable);
		R result = delegate.execute();
		deleteListener.afterDelete(iterable);
		return result;
	}
	
	public SelectListenerCollection<T> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<T> addSelectListener(ISelectListener<T> selectListener) {
		this.selectListener.add(selectListener);
		return this;
	}
	
	public List<T> doWithSelectListener(Iterable<Serializable> ids, ISilentDelegate<List<T>> delegate) {
		selectListener.beforeSelect(ids);
		List<T> toReturn = delegate.execute();
		selectListener.afterSelect(toReturn);
		return toReturn;
	}
	
}
