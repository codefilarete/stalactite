package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListenerCollection;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.result.Row;

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
	public void update(I id, Consumer<C> entityConsumer) {
		surrogate.update(id, entityConsumer);
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
	public EntityMappingStrategy<C, I, ?> getMappingStrategy() {
		return this.surrogate.getMappingStrategy();
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
	public void delete(Iterable<C> entities) {
		surrogate.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<C> entities) {
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
	public void updateById(Iterable<C> entities) {
		surrogate.updateById(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		surrogate.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, JID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, JID> leftColumn,
																				  Column<T2, JID> rightColumn,
																				  String rightTableAlias,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  boolean optional) {
		return surrogate.joinAsOne(sourcePersister, leftColumn, rightColumn, rightTableAlias, beanRelationFixer, optional);
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, SRCID, ID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																				  Column<T1, ID> leftColumn,
																				  Column<T2, ID> rightColumn,
																				  BeanRelationFixer<SRC, C> beanRelationFixer,
																				  @Nullable BiFunction<Row, ColumnedRow, ?> duplicateIdentifierProvider,
																				  String joinName,
																				  boolean optional,
																				  Set<Column<T2, ?>> selectableColumns) {
		return surrogate.joinAsMany(sourcePersister, leftColumn, rightColumn, beanRelationFixer, duplicateIdentifierProvider, joinName, optional, selectableColumns);
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
