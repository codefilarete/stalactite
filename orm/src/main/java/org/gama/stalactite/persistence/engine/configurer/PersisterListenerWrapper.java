package org.gama.stalactite.persistence.engine.configurer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * @author Guillaume Mary
 */
public class PersisterListenerWrapper<C, I> implements IEntityConfiguredPersister<C, I> {
	
	private final PersisterListener<C, I> persisterListener = new PersisterListener<>();
	private final IEntityConfiguredPersister<C, I> surrogate;
	
	public PersisterListenerWrapper(IEntityConfiguredPersister<C, I> surrogate) {
		this.surrogate = surrogate;
	}
	
	@Override
	public int persist(Iterable<C> entities) {
		return IEntityPersister.persist(entities, this::isNew, this, this, this, getMappingStrategy()::getId);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		return surrogate.selectWhere(getter, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		return surrogate.selectWhere(setter, operator);
	}
	
	@Override
	public List<C> selectAll() {
		return surrogate.selectAll();
	}
	
	@Override
	public boolean isNew(C entity) {
		return surrogate.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return surrogate.getClassToPersist();
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
	public IEntityMappingStrategy<C, I, ?> getMappingStrategy() {
		return this.surrogate.getMappingStrategy();
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		return this.surrogate.giveImpliedTables();
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
