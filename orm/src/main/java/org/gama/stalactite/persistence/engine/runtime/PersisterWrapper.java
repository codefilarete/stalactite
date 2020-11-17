package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * {@link IEntityConfiguredJoinedTablesPersister} that wraps another {@link IEntityConfiguredJoinedTablesPersister}.
 * Made to override only some targeted methods. 
 * 
 * @author Guillaume Mary
 */
public class PersisterWrapper<C, I> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	protected final IEntityConfiguredJoinedTablesPersister<C, I> surrogate;
	
	public PersisterWrapper(IEntityConfiguredJoinedTablesPersister<C, I> surrogate) {
		this.surrogate = surrogate;
	}
	
	public IEntityConfiguredJoinedTablesPersister<C, I> getSurrogate() {
		return surrogate;
	}
	
	/**
	 * Gets the last (in depth) delegate of this potential chain of wrapper
	 * @return at least the delegate of this instance
	 */
	public IEntityConfiguredJoinedTablesPersister<C, I> getDeepestSurrogate() {
		IEntityConfiguredJoinedTablesPersister<C, I> result = this;
		while(result instanceof PersisterWrapper && ((PersisterWrapper<C, I>) result).getSurrogate() != null) {
			result = ((PersisterWrapper<C, I>) result).getSurrogate();
		}
		return result;
	}
	
	@Override
	public int persist(Iterable<? extends C> entities) {
		return IEntityPersister.persist(entities, this::isNew, this, this, this, this::getId);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
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
	public int update(I id, Consumer<C> entityConsumer) {
		return surrogate.update(id, entityConsumer);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return surrogate.getClassToPersist();
	}
	
	@Override
	public void addInsertListener(InsertListener insertListener) {
		this.surrogate.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
		this.surrogate.addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
		this.surrogate.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
		this.surrogate.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
		this.surrogate.addDeleteByIdListener(deleteListener);
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
		return this.surrogate.getPersisterListener();
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		return surrogate.delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		return surrogate.deleteById(entities);
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		return surrogate.insert(entities);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return surrogate.select(ids);
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		return surrogate.updateById(entities);
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		return surrogate.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, JID> void joinAsOne(IJoinedTablesPersister<SRC, SRCID> sourcePersister,
																				Column<T1, JID> leftColumn,
																				Column<T2, JID> rightColumn,
																				BeanRelationFixer<SRC, C> beanRelationFixer,
																				boolean optional) {
		surrogate.joinAsOne(sourcePersister, leftColumn, rightColumn, beanRelationFixer, optional);
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID> void joinAsMany(IJoinedTablesPersister<SRC, SRCID> sourcePersister,
																			Column<T1, ?> leftColumn,
																			Column<T2, ?> rightColumn,
																			BeanRelationFixer<SRC, C> beanRelationFixer,
																			String joinName,
																			boolean optional) {
		surrogate.joinAsMany(sourcePersister, leftColumn, rightColumn, beanRelationFixer, joinName, optional);
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return surrogate.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		surrogate.copyRootJoinsTo(entityJoinTree, joinName);
	}
}
