package org.gama.stalactite.persistence.engine.listening;

import java.util.List;
import java.util.Map.Entry;

import org.gama.lang.bean.IQuietDelegate;

/**
 * Simple class that centralize persistence event listening. Delegates listening to encapsulated instance.
 * 
 * @author Guillaume Mary
 */
public class PersisterListener<T, I> {
	
	private InsertListenerCollection<T> insertListener = new InsertListenerCollection<>();
	private UpdateRoughlyListenerCollection<T> updateRoughlyListener = new UpdateRoughlyListenerCollection<>();
	private UpdateListenerCollection<T> updateListener = new UpdateListenerCollection<>();
	private DeleteListenerCollection<T> deleteListener = new DeleteListenerCollection<>();
	private DeleteRoughlyListenerCollection<T> deleteRoughlyListener = new DeleteRoughlyListenerCollection<>();
	private SelectListenerCollection<T, I> selectListener = new SelectListenerCollection<>();
	
	public InsertListenerCollection<T> getInsertListener() {
		return insertListener;
	}
	
	public PersisterListener<T, I> addInsertListener(IInsertListener<T> insertListener) {
		this.insertListener.add(insertListener);
		return this;
	}
	
	public <R> R doWithInsertListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		insertListener.beforeInsert(iterable);
		R result = delegate.execute();
		insertListener.afterInsert(iterable);
		return result;
	}
	
	public UpdateRoughlyListenerCollection<T> getUpdateRoughlyListener() {
		return updateRoughlyListener;
	}
	
	public PersisterListener<T, I> addUpdateRouglyListener(IUpdateRoughlyListener<T> updateRouglyListener) {
		this.updateRoughlyListener.add(updateRouglyListener);
		return this;
	}
	
	public <R> R doWithUpdateRouglyListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		updateRoughlyListener.beforeUpdateRoughly(iterable);
		R result = delegate.execute();
		updateRoughlyListener.afterUpdateRoughly(iterable);
		return result;
	}
	
	public UpdateListenerCollection<T> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<T, I> addUpdateListener(IUpdateListener<T> updateListener) {
		this.updateListener.add(updateListener);
		return this;
	}
	
	public <R> R doWithUpdateListener(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement, IQuietDelegate<R> delegate) {
		updateListener.beforeUpdate(differencesIterable, allColumnsStatement);
		R result = delegate.execute();
		updateListener.afterUpdate(differencesIterable, allColumnsStatement);
		return result;
	}
	
	public DeleteListenerCollection<T> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<T, I> addDeleteListener(IDeleteListener<T> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public <R> R doWithDeleteListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		deleteListener.beforeDelete(iterable);
		R result = delegate.execute();
		deleteListener.afterDelete(iterable);
		return result;
	}
	
	public PersisterListener<T, I> addDeleteRoughlyListener(IDeleteRoughlyListener<T> deleteRoughlyListener) {
		this.deleteRoughlyListener.add(deleteRoughlyListener);
		return this;
	}
	
	public <R> R doWithDeleteRoughlyListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		deleteRoughlyListener.beforeDeleteRoughly(iterable);
		R result = delegate.execute();
		deleteRoughlyListener.afterDeleteRoughly(iterable);
		return result;
	}
	
	public SelectListenerCollection<T, I> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<T, I> addSelectListener(ISelectListener<T, I> selectListener) {
		this.selectListener.add(selectListener);
		return this;
	}
	
	public List<T> doWithSelectListener(Iterable<I> ids, IQuietDelegate<List<T>> delegate) {
		selectListener.beforeSelect(ids);
		List<T> toReturn = delegate.execute();
		selectListener.afterSelect(toReturn);
		return toReturn;
	}
	
}
