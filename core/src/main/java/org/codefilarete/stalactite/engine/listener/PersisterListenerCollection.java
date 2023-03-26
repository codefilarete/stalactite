package org.codefilarete.stalactite.engine.listener;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.ThrowingExecutable;
import org.codefilarete.tool.function.ThrowingRunnable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

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
		R result;
		try {
			insertListener.beforeInsert(entities);
			result = delegate.execute();
			insertListener.afterInsert(entities);
		} catch (RuntimeException e) {
			insertListener.onError(entities, e);
			throw e;
		}
		return result;
	}
	
	public void doWithInsertListener(Iterable<? extends C> entities, ThrowingRunnable<RuntimeException> delegate) {
		try {
			insertListener.beforeInsert(entities);
			delegate.run();
			insertListener.afterInsert(entities);
		} catch (RuntimeException e) {
			insertListener.onError(entities, e);
			throw e;
		}
	}
	
	public UpdateByIdListenerCollection<C> getUpdateByIdListener() {
		return updateByIdListener;
	}
	
	public PersisterListenerCollection<C, I> addUpdateByIdListener(UpdateByIdListener<C> updateByIdListener) {
		this.updateByIdListener.add(updateByIdListener);
		return this;
	}
	
	public <R> R doWithUpdateByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		R result;
		try {
			updateByIdListener.beforeUpdateById(entities);
			result = delegate.execute();
			updateByIdListener.afterUpdateById(entities);
		} catch (RuntimeException e) {
			updateByIdListener.onError(entities, e);
			throw e;
		}
		return result;
	}
	
	public void doWithUpdateByIdListener(Iterable<C> entities, ThrowingRunnable<RuntimeException> delegate) {
		try {
			updateByIdListener.beforeUpdateById(entities);
			delegate.run();
			updateByIdListener.afterUpdateById(entities);
		} catch (RuntimeException e) {
			updateByIdListener.onError(entities, e);
			throw e;
		}
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
		R result;
		try {
			updateListener.beforeUpdate(differencesIterable, allColumnsStatement);
			result = delegate.apply(differencesIterable, allColumnsStatement);
			updateListener.afterUpdate(differencesIterable, allColumnsStatement);
		} catch (RuntimeException e) {
			updateListener.onError(Iterables.collectToList(differencesIterable, Duo::getLeft), e);
			throw e;
		}
		return result;
	}
	
	public <T extends Table<T>> void doWithUpdateListener(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement,
														  BiConsumer<Iterable<? extends Duo<C, C>>, Boolean> delegate) {
		try {
			updateListener.beforeUpdate(differencesIterable, allColumnsStatement);
			delegate.accept(differencesIterable, allColumnsStatement);
			updateListener.afterUpdate(differencesIterable, allColumnsStatement);
		} catch (RuntimeException e) {
			updateListener.onError(Iterables.collectToList(differencesIterable, Duo::getLeft), e);
			throw e;
		}
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
		R result;
		try {
			deleteListener.beforeDelete(entities);
			result = delegate.execute();
			deleteListener.afterDelete(entities);
		} catch (RuntimeException e) {
			deleteListener.onError(entities, e);
			throw e;
		}
		return result;
	}
	
	public void doWithDeleteListener(Iterable<C> entities, ThrowingRunnable<RuntimeException> delegate) {
		try {
			deleteListener.beforeDelete(entities);
			delegate.run();
			deleteListener.afterDelete(entities);
		} catch (RuntimeException e) {
			deleteListener.onError(entities, e);
			throw e;
		}
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<C> deleteByIdListener) {
		this.deleteByIdListener.add(deleteByIdListener);
	}
	
	public <R> R doWithDeleteByIdListener(Iterable<C> entities, ThrowingExecutable<R, RuntimeException> delegate) {
		R result;
		try {
			deleteByIdListener.beforeDeleteById(entities);
			result = delegate.execute();
			deleteByIdListener.afterDeleteById(entities);
		} catch (RuntimeException e) {
			deleteByIdListener.onError(entities, e);
			throw e;
		}
		return result;
	}
	
	public void doWithDeleteByIdListener(Iterable<C> entities, ThrowingRunnable<RuntimeException> delegate) {
		try {
			deleteByIdListener.beforeDeleteById(entities);
			delegate.run();
			deleteByIdListener.afterDeleteById(entities);
		} catch (RuntimeException e) {
			deleteByIdListener.onError(entities, e);
			throw e;
		}
	}
	
	public SelectListenerCollection<C, I> getSelectListener() {
		return selectListener;
	}
	
	@Override
	public void addSelectListener(SelectListener<C, I> selectListener) {
		this.selectListener.add(selectListener);
	}
	
	public List<C> doWithSelectListener(Iterable<I> ids, ThrowingExecutable<List<C>, RuntimeException> delegate) {
		List<C> result;
		try {
			selectListener.beforeSelect(ids);
			result = delegate.execute();
			selectListener.afterSelect(result);
		} catch (RuntimeException e) {
			selectListener.onError(ids, e);
			throw e;
		}
		return result;
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
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
