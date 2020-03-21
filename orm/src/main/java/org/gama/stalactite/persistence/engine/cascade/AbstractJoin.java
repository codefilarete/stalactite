package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType.INNER;

/**
 * The "right part" of a join between between 2 {@link IEntityMappingStrategy}s
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJoin<O> {
	/** The right part of the join */
	protected final StrategyJoins<O, ?> strategy;
	/** Join column with previous strategy table */
	protected final Column leftJoinColumn;
	/** Join column with next strategy table */
	protected final Column rightJoinColumn;
	/** Indicates if the join must be an inner or (left) outer join */
	private final JoinType joinType;
	
	public AbstractJoin(StrategyJoins<O, ?> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType) {
		this.strategy = strategy;
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
	}
	
	public AbstractJoin(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn) {
		this(strategy, leftJoinColumn, rightJoinColumn, INNER);
	}
	
	public AbstractJoin(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn, JoinType joinType) {
		this.strategy = new StrategyJoins<>(strategy);
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
	}
	
	public StrategyJoins<O, Table> getStrategy() {
		return (StrategyJoins<O, Table>) strategy;
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
	
	public abstract AbstractJoin<O> copyTo(Column leftJoinColumn);
	
	public enum JoinType {
		INNER,
		OUTER
	}
}
