package org.codefilarete.stalactite.sql.ddl;

/**
 * Stores the precision and scale of a fixed point number data.
 *
 * @author Guillaume Mary
 */
public class FixedPoint implements Size {
	
	/**
	 * Precision is the number of digits in a number.
	 */
	private final int precision;
	
	/**
	 * Scale is the number of digits to the right of the decimal point in a number
	 */
	private final Integer scale;
	
	public FixedPoint(int precision, Integer scale) {
		this.precision = precision;
		this.scale = scale;
	}
	
	public int getPrecision() {
		return precision;
	}
	
	public Integer getScale() {
		return scale;
	}
}