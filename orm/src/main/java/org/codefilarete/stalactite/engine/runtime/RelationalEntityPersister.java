package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Contract to allow joining a persister with another for entities with relation.
 * 
 * @author Guillaume Mary
 */
public interface RelationalEntityPersister<C, I> extends EntityPersister<C, I> {
	
	/**
	 * Called to join this instance with given persister. For this method, current instance is considered as the "right part" of the relation.
	 * Made as such because polymorphic cases (which are instance of this interface) are the only one who knows how to join themselves with another persister.
	 * 
	 * @param <SRC> source entity type
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param rightTableAlias optional alias for right table, if null table name will be used
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 * @return the created join name, then it could be found in sourcePersister#getEntityJoinTree
	 */
	<SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																			  Key<T1, JOINID> leftColumn,
																			  Key<T2, JOINID> rightColumn,
																			  String rightTableAlias,
																			  BeanRelationFixer<SRC, C> beanRelationFixer,
																			  boolean optional,
																			  boolean loadSeparately);
	
	/**
	 * Called to join this instance with given persister. For this method, current instance is considered as the "right part" of the relation.
	 * Made as such because polymorphic cases (which are instance of this interface) are the only one who knows how to join themselves with another persister.
	 *
	 * @param <SRC> source entity type
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance, expected to manage collection instantiation
	 * @param duplicateIdentifierProvider a function that computes the relation identifier
	 * @param joinName parent join node name on which join must be added,
	 * not always {@link EntityJoinTree#ROOT_STRATEGY_NAME} in particular in one-to-many with association table
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 */
	default <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							   Key<T1, JOINID> leftColumn,
																							   Key<T2, JOINID> rightColumn,
																							   BeanRelationFixer<SRC, C> beanRelationFixer,
																							   @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
																							   String joinName,
																							   boolean optional,
																							   boolean loadSeparately) {
		return joinAsMany(sourcePersister, leftColumn, rightColumn, beanRelationFixer, duplicateIdentifierProvider,
				joinName, Collections.emptySet(), optional, loadSeparately);
	}
	
	/**
	 * Called to join this instance with given persister. For this method, current instance is considered as the "right part" of the relation.
	 * Made as such because polymorphic cases (which are instance of this interface) are the only one who knows how to join themselves with another persister.
	 *
	 * @param <SRC> source entity type
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance, expected to manage collection instantiation
	 * @param duplicateIdentifierProvider a function that computes the relation identifier
	 * @param joinName parent join node name on which join must be added,
	 * not always {@link EntityJoinTree#ROOT_STRATEGY_NAME} in particular in one-to-many with association table
	 * @param selectableColumns columns to be added to SQL select clause
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 */
	<SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																					   Key<T1, JOINID> leftColumn,
																					   Key<T2, JOINID> rightColumn,
																					   BeanRelationFixer<SRC, C> beanRelationFixer,
																					   @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
																					   String joinName,
																					   Set<? extends Column<T2, ?>> selectableColumns,
																					   boolean optional,
																					   boolean loadSeparately);
	
	EntityJoinTree<C, I> getEntityJoinTree();
	
	/**
	 * Copies current instance joins root to given select
	 * 
	 * @param entityJoinTree target of the copy
	 * @param joinName name of target select join on which joins of this instance must be copied
	 * @param <E> target select entity type
	 * @param <ID> identifier type
	 */
	<E, ID> void copyRootJoinsTo(EntityJoinTree<E, ID> entityJoinTree, String joinName);
	
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(getter, operator);
	}
	
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(setter, operator);
	}
	
	default <O, A> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableFunction<C, A> getter1, SerializableFunction<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.chain(getter1, getter2), operator);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * Please note that whole bean graph is loaded, not only entities that satisfies criteria.
	 *
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 * @throws Exception if the column matching targeted property can't be found in entity mapping
	 */
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(AccessorChain<C, O> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(accessorChain, operator);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * Please note that whole bean graph is loaded, not only entities that satisfies criteria.
	 *
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 * @throws Exception if the column matching targeted property can't be found in entity mapping
	 */
	ExecutableEntityQueryCriteria<C, ?> selectWhere();
	
	/**
	 * Register a relation to another persister. Made to make {@link #selectWhere(SerializableFunction, ConditionalOperator)} methods working.
	 * Called at a very late stage of persister configuration when all persisters are available. 
	 */
	void registerRelation(ValueAccessPoint<C> relation, ConfiguredRelationalPersister<?, ?> persister);
	
	/**
	 * Gives the column on which the last element of the given accessor chain is persisted.
	 * The first element of the given accessor chain is expected to match a property of this persister.
	 * The lookup will go down the tree / graph of persistence.
	 * 
	 * @param accessorChain a suite of accessor describing a property of current persisted class
	 * @return the column matching the property, will throw an exception if a property of the chain is not mapped or found by this persister
	 */
	Column getColumn(List<? extends ValueAccessPoint<?>> accessorChain);
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	interface ExecutableEntityQueryCriteria<C, SELF extends ExecutableEntityQueryCriteria<C, SELF>>
			extends ExecutableEntityQuery<C, SELF>, RelationalEntityCriteria<C, SELF>, OrderByChain<C, SELF> {
		
	}
}
