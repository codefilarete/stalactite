package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeJoinPoint.JoinType;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeRowTransformer.EntityInflater;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Combination of {@link IEntityMappingStrategy} as a tree formed by joins of a strategies: owns the left part of the join, and "right parts"
 * are represented by a collection of {@link EntityMappingStrategyTreeJoinPoint}, ending up with a tree of joins representing a relation graph.
 *
 * @param <E> the type of the entity mapped by the {@link ClassMappingStrategy}
 */
public class EntityMappingStrategyTree<E, D extends Table> {
	/** The left part of the join, nullable in case of passive join (such as association table) */
	@Nullable
	private final EntityInflater<E, ?> strategy;
	private final D table;
	private final Set<Column<D, Object>> columnsToSelect;
	/** Joins */
	private final List<EntityMappingStrategyTreeJoinPoint> joins = new ArrayList<>();
	
	@Nullable
	private String tableAlias;
	
	EntityMappingStrategyTree(IEntityMappingStrategy<E, ?, D> root) {
		this(root, null);
	}
	
	EntityMappingStrategyTree(IEntityMappingStrategy<E, ?, D> root, String tableAlias) {
		this(new EntityInflater<E, Object>() {
			
			@Override
			public Class<E> getEntityType() {
				return root.getClassToPersist();
			}
			
			@Override
			public Object giveIdentifier(Row row, ColumnedRow columnedRow) {
				return root.getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow);
			}
			
			@Override
			public AbstractTransformer copyTransformerWithAliases(ColumnedRow columnedRow) {
				return root.copyTransformerWithAliases(columnedRow);
			}
		}, root.getTargetTable(), root.getSelectableColumns(), tableAlias);
	}
	
	EntityMappingStrategyTree(@Nullable EntityInflater<E, ?> strategy, D table, Set<Column<D, Object>> columnsToSelect, @Nullable String tableAlias) {
		this.strategy = strategy;
		this.table = table;
		this.columnsToSelect = columnsToSelect;
		this.tableAlias = tableAlias;
	}
	
	/**
	 * @return the left part of the join, nullable in case of passive join (such as association table)
	 */
	@Nullable
	public <I> EntityInflater<E, I> getStrategy() {
		return (EntityInflater<E, I>) strategy;
	}
	
	public List<EntityMappingStrategyTreeJoinPoint> getJoins() {
		return joins;
	}
	
	public Table getTable() {
		return table;
	}
	
	public Set<Column<D, Object>> getColumnsToSelect() {
		return columnsToSelect;
	}
	
	@Nullable
	public String getTableAlias() {
		return tableAlias;
	}
	
	/**
	 * Forces the table alias of the strategy table in the select statement
	 * @param tableAlias must be sql compliant (none checked)
	 */
	public void setTableAlias(@Nullable String tableAlias) {
		this.tableAlias = tableAlias;
	}
	
	/**
	 * Creates a join between this strategy and the given one.
	 * Method used for one-to-one and one-to-many (owned by reverse side) relations
	 * 
	 * @param strategy the new strategy on which to join
	 * @param leftJoinColumn the column of the owned strategy table (no check done) on which the join will be made
	 * @param rightJoinColumn the column of the new strategy table (no check done) on which the join will be made
	 * @param joinType indicates if the join is an outer (left) one or not
	 * @param beanRelationFixer will help to apply the instance of the new strategy on the owned one
	 * @return the created join
	 */
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> RelationJoin<U> add(IEntityMappingStrategy<U, ID, T2> strategy,
																			Column<T1, ID> leftJoinColumn,
																			Column<T2, ID> rightJoinColumn,
																			JoinType joinType,
																			BeanRelationFixer beanRelationFixer) {
		RelationJoin<U> join = new RelationJoin<>(strategy, leftJoinColumn, rightJoinColumn, joinType, beanRelationFixer);
		this.joins.add(join);
		return join;
	}
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> RelationJoin<U> add(EntityInflater<U, ID> strategy,
																			Set<Column<Table, Object>> columnsToSelect,
																			Column<T1, ID> leftJoinColumn,
																			Column<T2, ID> rightJoinColumn,
																			JoinType joinType,
																			BeanRelationFixer beanRelationFixer) {
		RelationJoin<U> join = new RelationJoin<>(strategy, columnsToSelect, leftJoinColumn, rightJoinColumn, joinType, beanRelationFixer);
		this.joins.add(join);
		return join;
	}

	/**
	 * Creates a join between this strategy and the given one.
	 * Method used for inheritance cases to complete current strategy with given one
	 *
	 * @param strategy the new strategy on which to join
	 * @param leftJoinColumn the column of the owned strategy table (no check done) on which the join will be made
	 * @param rightJoinColumn the column of the new strategy table (no check done) on which the join will be made
	 * @return the created join
	 */
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> MergeJoin addMergeJoin(IEntityMappingStrategy<U, ID, T2> strategy,
																			   Column<T1, ID> leftJoinColumn,
																			   Column<T2, ID> rightJoinColumn) {
		MergeJoin<U> join = new MergeJoin<>(strategy, leftJoinColumn, rightJoinColumn);
		this.joins.add(join);
		return join;
	}
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> MergeJoin addMergeJoin(EntityInflater<U, ID> strategy,
																			   Set<Column<Table, Object>> columnsToSelect,
																			   Column<T1, ID> leftJoinColumn,
																			   Column<T2, ID> rightJoinColumn,
																			   JoinType joinType) {
		MergeJoin<U> join = new MergeJoin<>(strategy, columnsToSelect, leftJoinColumn, rightJoinColumn, joinType);
		this.joins.add(join);
		return join;
	}
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> PassiveJoin addPassiveJoin(Column<T1, ID> leftJoinColumn,
																				   Column<T2, ID> rightJoinColumn,
																				   JoinType joinType,
																				   Set<Column<Table, Object>> columnsToSelect) {
		PassiveJoin<U> join = new PassiveJoin<>(leftJoinColumn, rightJoinColumn, joinType, columnsToSelect);
		this.joins.add(join);
		return join;
	}
	
	/**
	 * Copies this instance into given target at given join node name.
	 * Used to plug this graph as a subgraph of a join node of the given target.
	 * 
	 * @param target the graph that will contain this subgraph after copy
	 * @param targetJoinsNodeName join node target
	 */
	public void copyTo(EntityMappingStrategyTreeSelectBuilder target, String targetJoinsNodeName) {
		target.getTree(targetJoinsNodeName).joins.addAll(this.joins);
	}
	
	/**
	 * Almost the same as {@link #copyTo(EntityMappingStrategyTreeSelectBuilder, String)} but replaces left join columns (of this) by columns of
	 * the targeted select table, this allows to copy current joins to another select table which is not the same as the current one. 
	 *
	 * @param target the graph that will contain this subgraph after copy
	 * @param targetJoinsNodeName join node target
	 */
	public void projectTo(EntityMappingStrategyTreeSelectBuilder target, String targetJoinsNodeName) {
		EntityMappingStrategyTree targetJoins = target.getTree(targetJoinsNodeName);
		List<EntityMappingStrategyTreeJoinPoint> projectedJoins = Iterables.collectToList(this.joins, abstractJoin ->
			abstractJoin.copyTo(targetJoins.getTable().getColumn(abstractJoin.getLeftJoinColumn().getName()))
		);
		targetJoins.joins.addAll(projectedJoins);
	}
	
	/**
	 * Specilization of an {@link EntityMappingStrategyTreeJoinPoint} for relation between beans such as one-to-one or one-to-many
	 */
	public static class RelationJoin<O> extends EntityMappingStrategyTreeJoinPoint<O> {
		/** Relation fixer for instances of this strategy on owning strategy entities */
		private final BeanRelationFixer beanRelationFixer;
		
		private RelationJoin(EntityMappingStrategyTree<O, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType, BeanRelationFixer beanRelationFixer) {
			super(strategy, leftJoinColumn, rightJoinColumn, joinType);
			this.beanRelationFixer = beanRelationFixer;
		}
		
		private RelationJoin(EntityInflater<O, ?> strategy, Set<Column<Table, Object>> columnsToSelect, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType, BeanRelationFixer beanRelationFixer) {
			super(new EntityMappingStrategyTree<>(strategy, leftJoinColumn.getTable(), columnsToSelect, null), leftJoinColumn, rightJoinColumn, joinType);
			this.beanRelationFixer = beanRelationFixer;
		}
		
		private RelationJoin(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType, BeanRelationFixer beanRelationFixer) {
			super(strategy, leftJoinColumn, rightJoinColumn, joinType);
			this.beanRelationFixer = beanRelationFixer;
		}
		
		public BeanRelationFixer getBeanRelationFixer() {
			return beanRelationFixer;
		}
		
		@Override
		public EntityMappingStrategyTreeJoinPoint<O> copyTo(Column leftJoinColumn) {
			return new RelationJoin(super.getStrategy(), leftJoinColumn, getRightJoinColumn(), getJoinType(), getBeanRelationFixer());
		}
		
	}
	
	/**
	 * Specilization of an {@link EntityMappingStrategyTreeJoinPoint} for cases of inheritance/polymorphism by joined tables : not need to apply relationship setter.
	 * Join will be done with inner kind one because its seems nonesense to have optional merge information in inheritance cases. 
	 * By itself it indicates that its strategy table content must be merged with its parent {@link EntityMappingStrategyTree}.
	 */
	public static class MergeJoin<O> extends EntityMappingStrategyTreeJoinPoint<O> {
		
		private MergeJoin(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn) {
			super(strategy, leftJoinColumn, rightJoinColumn);
		}
		
		private MergeJoin(EntityInflater<O, ?> strategy, Set<Column<Table, Object>> columnsToSelect, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType) {
			super(new EntityMappingStrategyTree<>(strategy, leftJoinColumn.getTable(), columnsToSelect, null), leftJoinColumn, rightJoinColumn, joinType);
		}
		
		@Override
		public EntityMappingStrategyTreeJoinPoint<O> copyTo(Column leftJoinColumn) {
			return new MergeJoin<>(super.getStrategy().getStrategy(), super.getStrategy().getColumnsToSelect(), leftJoinColumn, getRightJoinColumn(), getJoinType());
		}
	}
	
	public static class PassiveJoin<O> extends EntityMappingStrategyTreeJoinPoint<O> {
		
		private PassiveJoin(Column leftJoinColumn, Column rightJoinColumn, JoinType joinType, Set<Column<Table, Object>> columnsToSelect) {
			super(new EntityMappingStrategyTree<>(null, rightJoinColumn.getTable(), columnsToSelect, null), leftJoinColumn, rightJoinColumn, joinType);
		}
		
		@Override
		public EntityMappingStrategyTreeJoinPoint<O> copyTo(Column leftJoinColumn) {
			return new PassiveJoin<>(leftJoinColumn, getRightJoinColumn(), getJoinType(), super.getStrategy().getColumnsToSelect());
		}
	}
}
