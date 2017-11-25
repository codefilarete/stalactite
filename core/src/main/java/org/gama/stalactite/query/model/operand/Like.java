package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.query.model.Operand;

/**
 * @author Guillaume Mary
 */
public class Like extends Operand {
	
	private boolean leadingStar;
	private boolean endingStar;
	
	public Like(Object value) {
		this(value, false, false);
	}
	
	public Like(Object value, boolean leadingStar, boolean endingStar) {
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
