package org.gama.stalactite.query.model;

/**
 * 
 * @param <V> dealing-with value type (not always value type)
 * @author Guillaume Mary
 */
// TODO : to be renamed in RelationalOperator
public abstract class AbstractOperator<V> {
	
	/** Is this operator must be negated ? */
	private boolean not;
	
	/**
	 * @return true if this operand uses "not"
	 */
	public boolean isNot() {
		return not;
	}
	
	/**
	 * Sets "not" value
	 * @param not true for this operand to use "not", false to let it use normal operand
	 */
	public void setNot(boolean not) {
		this.not = not;
	}
	
	/**
	 * Negates this operand
	 */
	public void setNot() {
		setNot(true);
	}
	
	/**
	 * Reverses logical operator
	 */
	public void switchNot() {
		this.not = !this.not;
	}
	
	public abstract boolean isNull();
}
