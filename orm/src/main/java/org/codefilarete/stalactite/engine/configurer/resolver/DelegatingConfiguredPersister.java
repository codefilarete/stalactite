package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Experimental;

/**
 * {@link ConfiguredPersister} that delegates its operations to a {@link DelegatingReadWriteEntityExecutor}.
 * Made to be compatible with {@link org.codefilarete.stalactite.sql.ddl.DDLDeployer} that needs to know the tables
 * involved in the persister : the {@link #giveImpliedTables()} method is implemented to return the tables given at
 * construction time.
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
class DelegatingConfiguredPersister<C, I> implements ConfiguredPersister<C, I> {
	
	private final DelegatingReadWriteEntityExecutor<C, I> delegate;
	private final Set<Table<?>> tables;
	
	public DelegatingConfiguredPersister(DelegatingReadWriteEntityExecutor<C, I> delegate, Set<? extends Table<?>> tables) {
		this.delegate = delegate;
		this.tables = new HashSet<>(tables);
	}
	
	@Override
	public Collection<Table<?>> giveImpliedTables() {
		return tables;
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return null;
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		return delegate.select(ids);
	}
	
	@Override
	public RelationalEntityPersister.ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return delegate.selectWhere();
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
		return delegate.selectProjectionWhere(selectAdapter);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		delegate.addSelectListener(selectListener);
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		delegate.persist(entities);
	}
	
	@Override
	public <T extends Table<T>> EntityMapping<C, I, T> getMapping() {
		return delegate.getMapping();
	}
	
	@Override
	public Set<C> selectAll() {
		return delegate.selectAll();
	}
	
	@Override
	public boolean isNew(C entity) {
		return delegate.isNew(entity);
	}
	
	@Override
	public I getId(C entity) {
		return delegate.getId(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return delegate.getClassToPersist();
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		delegate.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		delegate.deleteById(entities);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		delegate.insert(entities);
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		delegate.updateById(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		delegate.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		delegate.addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		delegate.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		delegate.addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		delegate.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		delegate.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		delegate.addDeleteByIdListener(deleteListener);
	}
	
	@Override
	public void persist(C entity) {
		delegate.persist(entity);
	}
	
	@Override
	public void insert(C...	 entity) {
		delegate.insert(entity);
	}
	
	@Override
	public void update(C modified, C unmodified, boolean allColumnsStatement) {
		delegate.update(modified, unmodified, allColumnsStatement);
	}
	
	@Override
	public void update(C entity) {
		delegate.update(entity);
	}
	
	@Override
	public void update(C entity, boolean allColumnsStatement) {
		delegate.update(entity, allColumnsStatement);
	}
	
	@Override
	public void update(Iterable<C> entities) {
		delegate.update(entities);
	}
	
	@Experimental
	@Override
	public void update(I id, Consumer<C> entityConsumer) {
		delegate.update(id, entityConsumer);
	}
	
	@Experimental
	@Override
	public void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		delegate.update(ids, entityConsumer);
	}
	
	@Override
	public void delete(C entity) {
		delegate.delete(entity);
	}
	
	@Override
	public void deleteById(C entity) {
		delegate.deleteById(entity);
	}
	
	@Override
	public C select(I id) {
		return delegate.select(id);
	}
	
	@Override
	public Set<C> select(I... ids) {
		return delegate.select(ids);
	}
	
	@Override
	public void updateById(C entity) {
		delegate.updateById(entity);
	}
	
	@Override
	public void persist(C... entities) {
		delegate.persist(entities);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(getter, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(setter, operator);
	}
	
	@Override
	public <O, A> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(getter1, getter2, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C, ?> selectWhere(List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(accessorChain, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C, ?> selectWhere(AccessorChain<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(accessorChain, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return delegate.selectWhere(accessorChain, operator);
	}
	
	@Override
	public <O, S extends Collection<O>, NEXT> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1, SerializablePropertyAccessor<O, NEXT> accessor2, ConditionalOperator<NEXT, ?> operator) {
		return delegate.selectWhere(accessor1, accessor2, operator);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, getter, operator);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, setter, operator);
	}
	
	@Override
	public <O, A> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, getter1, getter2, operator);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, accessorChain, operator);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, EntityCriteria.CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, accessorChain, operator);
	}
	
	@Override
	public <O, S extends Collection<O>, NEXT> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1, SerializablePropertyAccessor<O, NEXT> accessor2, ConditionalOperator<O, ?> operator) {
		return delegate.selectProjectionWhere(selectAdapter, accessor1, accessor2, operator);
	}
}
