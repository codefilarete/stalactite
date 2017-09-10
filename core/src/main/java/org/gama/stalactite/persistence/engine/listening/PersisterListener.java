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
	private UpdateByIdListenerCollection<T> updateByIdListenerCollection = new UpdateByIdListenerCollection<>();
	private UpdateListenerCollection<T> updateListener = new UpdateListenerCollection<>();
	private DeleteListenerCollection<T> deleteListener = new DeleteListenerCollection<>();
	private DeleteByIdListenerCollection<T> deleteByIdListenerCollection = new DeleteByIdListenerCollection<>();
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
	
	public UpdateByIdListenerCollection<T> getUpdateByIdListenerCollection() {
		return updateByIdListenerCollection;
	}
	
	public PersisterListener<T, I> addUpdateByIdListener(IUpdateByIdListener<T> updateByIdListener) {
		this.updateByIdListenerCollection.add(updateByIdListener);
		return this;
	}
	
	public <R> R doWithUpdateByIdListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		updateByIdListenerCollection.beforeUpdateById(iterable);
		R result = delegate.execute();
		updateByIdListenerCollection.afterUpdateById(iterable);
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
	
	public PersisterListener<T, I> addDeleteByIdListener(IDeleteByIdListener<T> deleteByIdListener) {
		this.deleteByIdListenerCollection.add(deleteByIdListener);
		return this;
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<T> iterable, IQuietDelegate<R> delegate) {
		deleteByIdListenerCollection.beforeDeleteById(iterable);
		R result = delegate.execute();
		deleteByIdListenerCollection.afterDeleteById(iterable);
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
