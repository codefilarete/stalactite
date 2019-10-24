package org.gama.stalactite.persistence.engine.listening;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.ThrowingExecutable;
import org.gama.stalactite.persistence.structure.Table;

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
	
	public <R> R doWithInsertListener(Iterable<? extends C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
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
	
	public <R> R doWithUpdateByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		updateByIdListenerCollection.beforeUpdateById(entities);
		R result = execute(delegate, entities, updateByIdListenerCollection::onError);
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
	
	public <R, T extends Table<T>> R doWithUpdateListener(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement,
														  BiFunction<Iterable<? extends Duo<? extends C, ? extends C>>, Boolean, R> delegate) {
		updateListener.beforeUpdate(differencesIterable, allColumnsStatement);
		R result;
		try {
			result = delegate.apply(differencesIterable, allColumnsStatement);
		} catch (RuntimeException e) {
			updateListener.onError(Iterables.collectToList(differencesIterable, Duo::getLeft), e);
			throw e;
		}
		updateListener.afterUpdate(differencesIterable, allColumnsStatement);
		return result;
	}
	
	public DeleteListenerCollection<C> getDeleteListener() {
		return deleteListener;
	}
	
	public PersisterListener<C, I> addDeleteListener(DeleteListener<C> deleteListener) {
		this.deleteListener.add(deleteListener);
		return this;
	}
	
	public <R> R doWithDeleteListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		deleteListener.beforeDelete(entities);
		R result = execute(delegate, entities, deleteListener::onError);
		deleteListener.afterDelete(entities);
		return result;
	}
	
	public PersisterListener<C, I> addDeleteByIdListener(DeleteByIdListener<C> deleteByIdListener) {
		this.deleteByIdListenerCollection.add(deleteByIdListener);
		return this;
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		deleteByIdListenerCollection.beforeDeleteById(entities);
		R result = execute(delegate, entities, deleteByIdListenerCollection::onError);
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
	
	public List<C> doWithSelectListener(Iterable<I> ids, ThrowingExecutable<List<C>, RuntimeException> delegate) {
		selectListener.beforeSelect(ids);
		List<C> result = execute(delegate, ids, selectListener::onError);
		selectListener.afterSelect(result);
		return result;
	}
	
	private <X, R> R execute(ThrowingExecutable<R, RuntimeException> delegate, Iterable<X> entities,
								 BiConsumer<Iterable<X>, RuntimeException> errorHandler) {
		try {
			return delegate.execute();
		} catch (RuntimeException e) {
			errorHandler.accept(entities, e);
			throw e;
		}
	}
}
