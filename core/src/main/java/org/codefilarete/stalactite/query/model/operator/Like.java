package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a like comparison.
 * Options to create a "startsWith" and "endsWith" are provided.
 * 
 * @author Guillaume Mary
 */
public class Like extends UnitaryOperator<CharSequence> {
	
	private final boolean leadingStar;
	private final boolean endingStar;
	
	/**
	 * Basic constructor
	 * @param value something that looks like a String, may contain '%' characters
	 */
	public Like(CharSequence value) {
		this(value, false, false);
	}
	
	/**
	 * Constructor for "startsWith" and "endsWith" operator
	 * @param value something that looks like a String, may contain '%' characters
	 * @param leadingStar true to add a leading generic '%' character
	 * @param endingStar true to add a ending generic '%' character
	 */
	public Like(CharSequence value, boolean leadingStar, boolean endingStar) {
		super(value);
		this.leadingStar = leadingStar;
		this.endingStar = endingStar;
	}
	
	public boolean withLeadingStar() {
		return leadingStar;
	}
	
	public boolean withEndingStar() {
		return endingStar;
	}
}
