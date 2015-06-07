package org.gama.stalactite.persistence.engine.listening;

import org.gama.lang.bean.IDelegate;
import org.gama.lang.bean.IDelegateWithResult;

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
	
	public void doWithInsertListener(Iterable<T> iterable, IDelegate delegate) {
		insertListener.beforeInsert(iterable);
		delegate.execute();
		insertListener.afterInsert(iterable);
	}
	
	public UpdateRouglyListenerCollection<T> getUpdateRouglyListener() {
		return updateRouglyListener;
	}
	
	public PersisterListener<T> addUpdateRouglyListener(IUpdateRouglyListener<T> updateRouglyListener) {
		this.updateRouglyListener.add(updateRouglyListener);
		return this;
	}
	
	public void doWithUpdateRouglyListener(Iterable<T> iterable, IDelegate delegate) {
		updateRouglyListener.beforeUpdateRoughly(iterable);
		delegate.execute();
		updateRouglyListener.afterUpdateRoughly(iterable);
	}
	
	public UpdateListenerCollection<T> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<T> addUpdateListener(IUpdateListener<T> updateListener) {
		this.updateListener.add(updateListener);
		return this;
	}
	
	public void doWithUpdateListener(Iterable<Entry<T, T>> differencesIterable, IDelegate delegate) {
		updateListener.beforeUpdate(differencesIterable);
		delegate.execute();
		updateListener.afterUpdate(differencesIterable);
	}
	
	public DeleteListenerCollection<T> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<T> addDeleteListener(IDeleteListener<T> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public void doWithDeleteListener(Iterable<T> iterable, IDelegate delegate) {
		deleteListener.beforeDelete(iterable);
		delegate.execute();
		deleteListener.afterDelete(iterable);
	}
	
	public SelectListenerCollection<T> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<T> addSelectListener(ISelectListener<T> selectListener) {
		this.selectListener.add(selectListener);
		return this;
	}
	
	public List<T> doWithSelectListener(Iterable<Serializable> ids, IDelegateWithResult<List<T>> delegate) {
		selectListener.beforeSelect(ids);
		List<T> tList = delegate.execute();
		selectListener.afterSelect(tList);
		return tList;
	}
	
}
