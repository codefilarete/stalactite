package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.Duo;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * {@link EntityConfiguredJoinedTablesPersister} that wraps another {@link EntityConfiguredJoinedTablesPersister}.
 * Made to override only some targeted methods. 
 * 
 * @author Guillaume Mary
 */
public class PersisterWrapper<C, I> implements EntityConfiguredJoinedTablesPersister<C, I> {
	
	protected final EntityConfiguredJoinedTablesPersister<C, I> surrogate;
	
	public PersisterWrapper(EntityConfiguredJoinedTablesPersister<C, I> surrogate) {
		this.surrogate = surrogate;
	}
	
	public EntityConfiguredJoinedTablesPersister<C, I> getSurrogate() {
		return surrogate;
	}
	
	/**
	 * Gets the last (in depth) delegate of this potential chain of wrapper
	 * @return at least the delegate of this instance
	 */
	public EntityConfiguredJoinedTablesPersister<C, I> getDeepestSurrogate() {
		EntityConfiguredJoinedTablesPersister<C, I> result = this;
		while(result instanceof PersisterWrapper && ((PersisterWrapper<C, I>) result).getSurrogate() != null) {
			result = ((PersisterWrapper<C, I>) result).getSurrogate();
		}
		return result;
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		EntityPersister.persist(entities, this::isNew, this, this, this, this::getId);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O> operator) {
		return surrogate.selectWhere(getter, operator);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator) {
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
	public void update(I id, Consumer<C> entityConsumer) {
		surrogate.update(id, entityConsumer);
	}
	
	@Override
	public void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		surrogate.update(ids, entityConsumer);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return surrogate.getClassToPersist();
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		this.surrogate.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		this.surrogate.addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		this.surrogate.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		this.surrogate.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		this.surrogate.addDeleteByIdListener(deleteListener);
	}
	
	@Override
	public EntityMapping<C, I, ?> getMapping() {
		return this.surrogate.getMapping();
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		return this.surrogate.giveImpliedTables();
	}
	
	@Override
	public PersisterListenerCollection<C, I> getPersisterListener() {
		return this.surrogate.getPersisterListener();
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		surrogate.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		surrogate.deleteById(entities);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		surrogate.insert(entities);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return surrogate.select(ids);
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		surrogate.updateById(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		surrogate.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																					 Key<T1, JOINID> leftColumn,
																					 Key<T2, JOINID> rightColumn,
																					 String rightTableAlias,
																					 BeanRelationFixer<SRC, C> beanRelationFixer,
																					 boolean optional,
																					 boolean loadSeparately) {
		return surrogate.joinAsOne(sourcePersister, leftColumn, rightColumn, rightTableAlias, beanRelationFixer, optional, loadSeparately);
	}
	
	@Override
	public <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							  Key<T1, JOINID> leftColumn,
																							  Key<T2, JOINID> rightColumn,
																							  BeanRelationFixer<SRC, C> beanRelationFixer,
																							  @Nullable BiFunction<Row, ColumnedRow, Object> duplicateIdentifierProvider,
																							  String joinName,
																							  Set<? extends Column<T2, Object>> selectableColumns,
																							  boolean optional,
																							  boolean loadSeparately) {
		return surrogate.joinAsMany(sourcePersister, leftColumn, rightColumn, beanRelationFixer, duplicateIdentifierProvider,
				joinName, selectableColumns, optional, loadSeparately);
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
