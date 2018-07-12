package org.gama.stalactite.persistence.engine.listening;

import java.util.List;
import java.util.Map.Entry;

import org.gama.lang.bean.IQuietDelegate;
import org.gama.lang.collection.Iterables;

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
	
	public <R> R doWithInsertListener(Iterable<T> entities, IQuietDelegate<R> delegate) {
		insertListener.beforeInsert(entities);
		R result;
		try {
			result = delegate.execute();
		} catch (RuntimeException e) {
			insertListener.onError(entities, e);
			throw e;
		}
		insertListener.afterInsert(entities);
		return result;
	}
	
	public UpdateByIdListenerCollection<T> getUpdateByIdListenerCollection() {
		return updateByIdListenerCollection;
	}
	
	public PersisterListener<T, I> addUpdateByIdListener(IUpdateByIdListener<T> updateByIdListener) {
		this.updateByIdListenerCollection.add(updateByIdListener);
		return this;
	}
	
	public <R> R doWithUpdateByIdListener(Iterable<T> entities, IQuietDelegate<R> delegate) {
		updateByIdListenerCollection.beforeUpdateById(entities);
		R result;
		try {
			result = delegate.execute();
		} catch (RuntimeException e) {
			updateByIdListenerCollection.onError(entities, e);
			throw e;
		}
		updateByIdListenerCollection.afterUpdateById(entities);
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
		R result;
		try {
			result = delegate.execute();
		} catch (RuntimeException e) {
			updateListener.onError(Iterables.collectToList(differencesIterable, Entry::getKey), e);
			throw e;
		}
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
	
		public <R> R doWithDeleteListener(Iterable<T> entities, IQuietDelegate<R> delegate) {
		deleteListener.beforeDelete(entities);
		R result;
		try {
			result = delegate.execute();
		} catch (RuntimeException e) {
			deleteListener.onError(entities, e);
			throw e;
		}
		deleteListener.afterDelete(entities);
		return result;
	}
	
	public PersisterListener<T, I> addDeleteByIdListener(IDeleteByIdListener<T> deleteByIdListener) {
		this.deleteByIdListenerCollection.add(deleteByIdListener);
		return this;
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<T> entities, IQuietDelegate<R> delegate) {
		deleteByIdListenerCollection.beforeDeleteById(entities);
		R result;
		try {
			result = delegate.execute();
		} catch (RuntimeException e) {
			deleteByIdListenerCollection.onError(entities, e);
			throw e;
		}
		deleteByIdListenerCollection.afterDeleteById(entities);
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
		List<T> toReturn;
		try {
			toReturn = delegate.execute();
		} catch (RuntimeException e) {
			selectListener.onError(ids, e);
			throw e;
		}
		selectListener.afterSelect(toReturn);
		return toReturn;
	}
	
}
