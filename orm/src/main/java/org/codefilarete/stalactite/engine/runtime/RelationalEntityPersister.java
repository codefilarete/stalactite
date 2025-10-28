package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Arrays;
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
	 * @param propertyAccessor accessor to the property of this persister's entity from the source entity type
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param rightTableAlias optional alias for right table, if null table name will be used
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 * @return the created join name, then it could be found in sourcePersister#getEntityJoinTree
	 */
	<SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsOne(RelationalEntityPersister<SRC, SRCID> sourcePersister,
																					  Accessor<SRC, C> propertyAccessor,
																					  Key<T1, JOINID> leftColumn,
																					  Key<T2, JOINID> rightColumn,
																					  @Nullable String rightTableAlias,
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
	 * @param propertyAccessor accessor to the property of this persister's entity from the source entity type
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance, expected to manage collection instantiation
	 * @param duplicateIdentifierProvider a function that computes the relation identifier
	 * @param joinName parent join node name on which join must be added,
	 * not always {@link EntityJoinTree#ROOT_JOIN_NAME} in particular in one-to-many with association table
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 */
	default <SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(String joinName,
																							   RelationalEntityPersister<SRC, SRCID> sourcePersister,
																							   Accessor<SRC, ?> propertyAccessor,
																							   Key<T1, JOINID> leftColumn,
																							   Key<T2, JOINID> rightColumn,
																							   BeanRelationFixer<SRC, C> beanRelationFixer,
																							   @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
																							   boolean optional,
																							   boolean loadSeparately) {
		return joinAsMany(joinName, sourcePersister, propertyAccessor, leftColumn, rightColumn, beanRelationFixer,
				duplicateIdentifierProvider, Collections.emptySet(), optional, loadSeparately);
	}
	
	/**
	 * Called to join this instance with given persister. For this method, current instance is considered as the "right part" of the relation.
	 * Made as such because polymorphic cases (which are instance of this interface) are the only one who knows how to join themselves with another persister.
	 *
	 * @param <SRC> source entity type
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param sourcePersister source that needs this instance joins
	 * @param propertyAccessor accessor to the property of this persister's entity from the source entity type
	 * @param leftColumn left part of the join, expected to be one of source table
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation of this instance onto source persister instance, expected to manage collection instantiation
	 * @param duplicateIdentifierProvider a function that computes the relation identifier
	 * @param joinName parent join node name on which join must be added,
	 * not always {@link EntityJoinTree#ROOT_JOIN_NAME} in particular in one-to-many with association table
	 * @param selectableColumns columns to be added to SQL select clause
	 * @param optional true for optional relation, makes an outer join, else should create a inner join
	 * @param loadSeparately indicator to make the target entities loaded in a separate query
	 */
	<SRC, T1 extends Table<T1>, T2 extends Table<T2>, SRCID, JOINID> String joinAsMany(String joinName,
																					   RelationalEntityPersister<SRC, SRCID> sourcePersister,
																					   Accessor<SRC, ?> propertyAccessor,
																					   Key<T1, JOINID> leftColumn,
																					   Key<T2, JOINID> rightColumn,
																					   BeanRelationFixer<SRC, C> beanRelationFixer,
																					   @Nullable Function<ColumnedRow, Object> duplicateIdentifierProvider,
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
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReference(getter), operator);
	}
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectWhere(Arrays.asList(Accessors.mutatorByMethodReference(setter)), operator);
	}
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	default <O, A> ExecutableEntityQueryCriteria<C, ?> selectWhere(SerializableFunction<C, A> getter1, SerializableFunction<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReferences(getter1, getter2), operator);
	}
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(accessorChain, operator);
	}
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	default <O> ExecutableEntityQueryCriteria<C, ?> selectWhere(AccessorChain<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(accessorChain, operator);
	}
	
	/**
	 * Overridden for a more accurate return type.
	 * {@inheritDoc}
	 */
	ExecutableEntityQueryCriteria<C, ?> selectWhere();
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	interface ExecutableEntityQueryCriteria<C, SELF extends ExecutableEntityQueryCriteria<C, SELF>>
			extends ExecutableEntityQuery<C, SELF>, RelationalEntityCriteria<C, SELF>, OrderByChain<C, SELF> {
		
	}
}
