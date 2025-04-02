package org.codefilarete.stalactite.sql.ddl;

/**
 * Class over the concept of size for SQL data types, like floating point or char sequence
 *
 * @author Guillaume Mary
 */
public interface Size {
	
	static Length length(int value) {
		return new Length(value);
	}
	
	static FixedPoint fixedPoint(int precision) {
		return fixedPoint(precision, null);
	}
	
	static FixedPoint fixedPoint(int precision, Integer scale) {
		return new FixedPoint(precision, scale);
	}
}