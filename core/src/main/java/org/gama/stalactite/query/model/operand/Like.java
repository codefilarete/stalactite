package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a like comparison.
 * Options to create a "startsWith" and "endsWith" are provided.
 * 
 * @author Guillaume Mary
 */
public class Like extends UnitaryOperator<CharSequence> {
	
	private boolean leadingStar;
	private boolean endingStar;
	
	/**
	 * Basic constructor
	 * @param value something that looks like a String, may contain '%' characters
	 */
	public Like(CharSequence value) {
		this(value, false, false);
	}
	
	/**
	 * Constructor for "startsWith" and "endsWith" operand
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
