package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.tool.collection.Arrays;

/**
 * Represents a "like" with ignore case comparison
 *
 * @author Guillaume Mary
 */
public class LikeIgnoreCase<O> extends BiOperandOperator<O, O> {
	
	private final boolean leadingStar;
	private final boolean endingStar;
	
	public LikeIgnoreCase() {
		this.leadingStar = true;
		this.endingStar = true;
	}
	
	public LikeIgnoreCase(O value) {
		super(value);
		this.leadingStar = true;
		this.endingStar = true;
	}
	
	public LikeIgnoreCase(Like<O> other) {
		super(other.getValue());
		setNot(other.isNot());
		this.leadingStar = other.withLeadingStar();
		this.endingStar = other.withEndingStar();
	}
	
	public boolean withLeadingStar() {
		return leadingStar;
	}
	
	public boolean withEndingStar() {
		return endingStar;
	}
	
	@Override
	public List<Object> asRawCriterion(Object leftOperand) {
		return Arrays.asList(
				new LowerCase<>(leftOperand),
				new Like<>(new LowerCase<>(getValue()), this.leadingStar, this.endingStar)
						.not(isNot()));
	}
}
