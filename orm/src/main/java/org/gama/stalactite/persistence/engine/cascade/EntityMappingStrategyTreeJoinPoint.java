package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.stalactite.persistence.engine.cascade.EntityMappingStrategyTreeJoinPoint.JoinType.INNER;

/**
 * The "right part" of a join between between 2 {@link IEntityMappingStrategy}s
 * 
 * @author Guillaume Mary
 */
public abstract class EntityMappingStrategyTreeJoinPoint<O> {
	/** The right part of the join */
	protected final EntityMappingStrategyTree<O, ?> strategy;
	/** Join column with previous strategy table */
	protected final Column leftJoinColumn;
	/** Join column with next strategy table */
	protected final Column rightJoinColumn;
	/** Indicates if the join must be an inner or (left) outer join */
	private final JoinType joinType;
	
	public EntityMappingStrategyTreeJoinPoint(EntityMappingStrategyTree<O, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType) {
		this.strategy = strategy;
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
	}
	
	public EntityMappingStrategyTreeJoinPoint(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn) {
		this(strategy, leftJoinColumn, rightJoinColumn, INNER);
	}
	
	public EntityMappingStrategyTreeJoinPoint(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType) {
		this.strategy = new EntityMappingStrategyTree<>(strategy);
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
	}
	
	public EntityMappingStrategyTree<O, Table> getStrategy() {
		return (EntityMappingStrategyTree<O, Table>) strategy;
	}
	
	public Column getLeftJoinColumn() {
		return leftJoinColumn;
	}
	
	public Column getRightJoinColumn() {
		return rightJoinColumn;
	}
	
	public JoinType getJoinType() {
		return joinType;
	}
	
	public abstract EntityMappingStrategyTreeJoinPoint<O> copyTo(Column leftJoinColumn);
	
	public enum JoinType {
		INNER,
		OUTER
	}
}
