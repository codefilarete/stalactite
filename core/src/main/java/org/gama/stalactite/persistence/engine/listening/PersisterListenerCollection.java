package org.gama.stalactite.persistence.engine.listening;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.ThrowingExecutable;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Simple class that centralizes persistence event listener registration as well as execution of some code wrapped between event triggering.
 * 
 * @author Guillaume Mary
 * @see #doWithInsertListener(Iterable, ThrowingExecutable)
 * @see #doWithUpdateListener(Iterable, boolean, BiFunction)
 * @see #doWithUpdateByIdListener(Iterable, ThrowingExecutable)
 * @see #doWithDeleteListener(Iterable, ThrowingExecutable)
 * @see #doWithDeleteByIdListener(Iterable, ThrowingExecutable)
 * @see #doWithSelectListener(Iterable, ThrowingExecutable)
 */
public class PersisterListenerCollection<C, I> implements PersisterListener<C, I> {
	
	private final InsertListenerCollection<C> insertListener = new InsertListenerCollection<>();
	private final UpdateByIdListenerCollection<C> updateByIdListener = new UpdateByIdListenerCollection<>();
	private final UpdateListenerCollection<C> updateListener = new UpdateListenerCollection<>();
	private final DeleteListenerCollection<C> deleteListener = new DeleteListenerCollection<>();
	private final DeleteByIdListenerCollection<C> deleteByIdListener = new DeleteByIdListenerCollection<>();
	private final SelectListenerCollection<C, I> selectListener = new SelectListenerCollection<>();
	
	public InsertListenerCollection<C> getInsertListener() {
		return insertListener;
	}
	
	@Override
	public void addInsertListener(InsertListener<C> insertListener) {
		this.insertListener.add(insertListener);
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
	
	public UpdateByIdListenerCollection<C> getUpdateByIdListener() {
		return updateByIdListener;
	}
	
	public PersisterListenerCollection<C, I> addUpdateByIdListener(UpdateByIdListener<C> updateByIdListener) {
		this.updateByIdListener.add(updateByIdListener);
		return this;
	}
	
	public <R> R doWithUpdateByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		updateByIdListener.beforeUpdateById(entities);
		R result = execute(delegate, entities, updateByIdListener::onError);
		updateByIdListener.afterUpdateById(entities);
		return result;
	}
	
	public UpdateListenerCollection<C> getUpdateListener() {
		return updateListener;
	}
	
	@Override
	public void addUpdateListener(UpdateListener<C> updateListener) {
		this.updateListener.add(updateListener);
	}
	
	public <R, T extends Table<T>> R doWithUpdateListener(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement,
														  BiFunction<Iterable<? extends Duo<C, C>>, Boolean, R> delegate) {
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
	
	public DeleteByIdListenerCollection<C> getDeleteByIdListener() {
		return deleteByIdListener;
	}
	
	@Override
	public void addDeleteListener(DeleteListener<C> deleteListener) {
		this.deleteListener.add(deleteListener);
	}
	
	public <R> R doWithDeleteListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		deleteListener.beforeDelete(entities);
		R result = execute(delegate, entities, deleteListener::onError);
		deleteListener.afterDelete(entities);
		return result;
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<C> deleteByIdListener) {
		this.deleteByIdListener.add(deleteByIdListener);
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		deleteByIdListener.beforeDeleteById(entities);
		R result = execute(delegate, entities, deleteByIdListener::onError);
		deleteByIdListener.afterDeleteById(entities);
		return result;
	}
	
	public SelectListenerCollection<C, I> getSelectListener() {
		return selectListener;
	}
	
	@Override
	public void addSelectListener(SelectListener<C, I> selectListener) {
		this.selectListener.add(selectListener);
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
	
	/**
	 * Move internal listeners to given instance.
	 * Usefull to agregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 * 
	 * @param persisterListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(PersisterListenerCollection<C, I> persisterListener) {
		this.insertListener.moveTo(persisterListener.insertListener);
		this.updateByIdListener.moveTo(persisterListener.updateByIdListener);
		this.updateListener.moveTo(persisterListener.updateListener);
		this.deleteListener.moveTo(persisterListener.deleteListener);
		this.deleteByIdListener.moveTo(persisterListener.deleteByIdListener);
		this.selectListener.moveTo(persisterListener.selectListener);
	}
}
