package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;

/**
 * {@link ConfiguredRelationalPersister} that wraps another {@link ConfiguredRelationalPersister}.
 * Made to override only some targeted methods. 
 * 
 * @author Guillaume Mary
 */
public class PersisterWrapper<C, I> implements ConfiguredRelationalPersister<C, I> {
	
	protected final ConfiguredRelationalPersister<C, I> delegate;
	
	public PersisterWrapper(ConfiguredRelationalPersister<C, I> delegate) {
		this.delegate = delegate;
	}
	
	public ConfiguredRelationalPersister<C, I> getDelegate() {
		return delegate;
	}
	
	/**
	 * Gets the last (in depth) delegate of this potential chain of wrapper
	 * @return at least the delegate of this instance
	 */
	public ConfiguredRelationalPersister<C, I> getDeepestDelegate() {
		ConfiguredRelationalPersister<C, I> result = this;
		while(result instanceof PersisterWrapper && ((PersisterWrapper<C, I>) result).getDelegate() != null) {
			result = ((PersisterWrapper<C, I>) result).getDelegate();
		}
		return result;
	}
	
	@Override
	public void registerRelation(ValueAccessPoint<C> relation, ConfiguredRelationalPersister<?, ?> persister) {
		this.delegate.registerRelation(relation, persister);
	}
	
	@Override
	public Column getColumn(List<? extends ValueAccessPoint<?>> accessorChain) {
		return delegate.getColumn(accessorChain);
	}
	
	@Override
	public I getId(C entity) {
		return delegate.getId(entity);
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return delegate.selectWhere();
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<Select> selectAdapter) {
		return delegate.selectProjectionWhere(selectAdapter);
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
	public void update(I id, Consumer<C> entityConsumer) {
		delegate.update(id, entityConsumer);
	}
	
	@Override
	public void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		delegate.update(ids, entityConsumer);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return delegate.getClassToPersist();
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		this.delegate.addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		this.delegate.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		this.delegate.addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		this.delegate.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		this.delegate.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		this.delegate.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		this.delegate.addDeleteByIdListener(deleteListener);
	}
	
	@Override
	public EntityMapping<C, I, ?> getMapping() {
		return this.delegate.getMapping();
	}
	
	@Override
	public Collection<Table<?>> giveImpliedTables() {
		return this.delegate.giveImpliedTables();
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return this.delegate.getPersisterListener();
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
	public void persist(Iterable<? extends C> entities) {
		delegate.persist(entities);
	}

	@Override
	public Set<C> select(Iterable<I> ids) {
		return delegate.select(ids);
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
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							 Accessor<SRC, C> propertyAccessor,
																							 Key<T1, JOINID> leftColumn,
																							 Key<T2, JOINID> rightColumn,
																							 String rightTableAlias,
																							 BeanRelationFixer<SRC, C> beanRelationFixer,
																							 boolean optional,
																							 boolean loadSeparately) {
		return delegate.joinAsOne(sourcePersister, propertyAccessor, leftColumn, rightColumn, rightTableAlias, beanRelationFixer, optional, loadSeparately);
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(String joinName,
																							  RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Accessor<SRC, ?> propertyAccessor,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
																							  Set<? extends Column<T2, ?>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		return delegate.joinAsMany(joinName, sourcePersister, propertyAccessor, leftColumn, rightColumn, beanRelationFixer,
				duplicateIdentifierProvider, selectableColumns, optional, loadSeparately);
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return delegate.getEntityJoinTree();
	}
	
	@Override
	public <E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName) {
		delegate.copyRootJoinsTo(entityJoinTree, joinName);
	}
}
