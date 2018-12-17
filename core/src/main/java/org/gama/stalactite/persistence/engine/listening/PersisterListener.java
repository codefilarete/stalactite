package org.gama.stalactite.persistence.engine.listening;

import java.util.List;
import java.util.function.BiFunction;

import org.gama.lang.Duo;
import org.gama.lang.bean.IQuietDelegate;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.function.Functions.chain;

/**
 * Simple class that centralize persistence event listening. Delegates listening to encapsulated instance.
 * 
 * @author Guillaume Mary
 */
public class PersisterListener<C, I> {
	
	private InsertListenerCollection<C> insertListener = new InsertListenerCollection<>();
	private UpdateByIdListenerCollection<C> updateByIdListenerCollection = new UpdateByIdListenerCollection<>();
	private UpdateListenerCollection<C> updateListener = new UpdateListenerCollection<>();
	private DeleteListenerCollection<C> deleteListener = new DeleteListenerCollection<>();
	private DeleteByIdListenerCollection<C> deleteByIdListenerCollection = new DeleteByIdListenerCollection<>();
	private SelectListenerCollection<C, I> selectListener = new SelectListenerCollection<>();
	
	public InsertListenerCollection<C> getInsertListener() {
		return insertListener;
	}
	
	public PersisterListener<C, I> addInsertListener(InsertListener<C> insertListener) {
		this.insertListener.add(insertListener);
		return this;
	}
	
	public <R> R doWithInsertListener(Iterable<? extends C> entities, IQuietDelegate<R> delegate) {
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
	
	public UpdateByIdListenerCollection<C> getUpdateByIdListenerCollection() {
		return updateByIdListenerCollection;
	}
	
	public PersisterListener<C, I> addUpdateByIdListener(UpdateByIdListener<C> updateByIdListener) {
		this.updateByIdListenerCollection.add(updateByIdListener);
		return this;
	}
	
	public <R> R doWithUpdateByIdListener(Iterable<C> entities, IQuietDelegate<R> delegate) {
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
	
	public UpdateListenerCollection<C> getUpdateListener() {
		return updateListener;
	}
	
	public PersisterListener<C, I> addUpdateListener(UpdateListener<C> updateListener) {
		this.updateListener.add(updateListener);
		return this;
	}
	
	public <R, T extends Table<T>> R doWithUpdateListener(Iterable<UpdatePayload<C, T>> differencesIterable, boolean allColumnsStatement,
														  BiFunction<Iterable<UpdatePayload<C, T>>, Boolean, R> delegate) {
		updateListener.beforeUpdate((Iterable) differencesIterable, allColumnsStatement);
		R result;
		try {
			result = delegate.apply(differencesIterable, allColumnsStatement);
		} catch (RuntimeException e) {
			updateListener.onError(Iterables.collectToList(differencesIterable, chain(UpdatePayload::getEntities, Duo::getLeft)), e);
			throw e;
		}
		updateListener.afterUpdate((Iterable) differencesIterable, allColumnsStatement);
		return result;
	}
	
	public DeleteListenerCollection<C> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<C, I> addDeleteListener(DeleteListener<C> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public <R> R doWithDeleteListener(Iterable<C> entities, IQuietDelegate<R> delegate) {
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
	
	public PersisterListener<C, I> addDeleteByIdListener(DeleteByIdListener<C> deleteByIdListener) {
		this.deleteByIdListenerCollection.add(deleteByIdListener);
		return this;
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<C> entities, IQuietDelegate<R> delegate) {
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
	
	public SelectListenerCollection<C, I> getSelectListener() {
		return selectListener;
	}
	
	public PersisterListener<C, I> addSelectListener(SelectListener<C, I> selectListener) {
		this.selectListener.add(selectListener);
		return this;
	}
	
	public List<C> doWithSelectListener(Iterable<I> ids, IQuietDelegate<List<C>> delegate) {
		selectListener.beforeSelect(ids);
		List<C> toReturn;
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
