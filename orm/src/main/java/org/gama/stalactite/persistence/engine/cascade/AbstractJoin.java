package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * The "right part" of a join between between 2 {@link IEntityMappingStrategy}s
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJoin<I, O> {
	/** The right part of the join */
	protected final StrategyJoins<O, I> strategy;
	/** Join column with previous strategy table */
	protected final Column leftJoinColumn;
	/** Join column with next strategy table */
	protected final Column rightJoinColumn;
	
	public AbstractJoin(IEntityMappingStrategy<O, ?, ? extends Table> strategy, Column leftJoinColumn, Column rightJoinColumn) {
		this.strategy = new StrategyJoins<>(strategy);
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
	}
	
	public StrategyJoins<O, I> getStrategy() {
		return strategy;
	}
	
	public Column getLeftJoinColumn() {
		return leftJoinColumn;
	}
	
	public Column getRightJoinColumn() {
		return rightJoinColumn;
	}
}
