package org.codefilarete.stalactite.query.model.operator;

import java.util.List;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.tool.collection.Arrays;

/**
 * Represents a like comparison.
 * Options to create a "startsWith" and "endsWith" are provided.
 * 
 * @author Guillaume Mary
 */
public class Like<V> extends UnitaryOperator<V> {
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "starts with" criterion.
	 * @param value something that looks like a String (may contain '%' characters)
	 * @return a new instance of {@link Like} that is a "starts with" criterion 
	 */
	public static <V extends CharSequence> Like<V> startsWith(V value) {
		return new Like<>(value, false, true);
	}
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "starts with" criterion without value for now : must be set
	 * later with {@link super#setValue(Object)}
	 * @return a new instance of {@link Like} that is a "starts with" criterion 
	 */
	public static <V extends CharSequence> Like<V> startsWith() {
		return new Like<>(false, true);
	}
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "ends with" criterion.
	 * @param value something that looks like a String (may contain '%' characters)
	 * @return a new instance of {@link Like} that is a "ends with" criterion 
	 */
	public static <V extends CharSequence> Like<V> endsWith(V value) {
		return new Like<>(value, true, false);
	}
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "ends with" criterion without value for now : must be set
	 * later with {@link super#setValue(Object)}
	 * @return a new instance of {@link Like} that is a "ends with" criterion 
	 */
	public static <V extends CharSequence> Like<V> endsWith() {
		return new Like<>(true, false);
	}
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "contains" criterion.
	 * @param value something that looks like a String (may contain '%' characters)
	 * @return a new instance of {@link Like} that is a "contains" criterion 
	 */
	public static <V extends CharSequence> Like<V> contains(V value) {
		return new Like<>(value, true, true);
	}
	
	/**
	 * Shortcut that builds a {@link Like} instance for a "contains" criterion without value for now : must be set
	 * later with {@link super#setValue(Object)}
	 * @return a new instance of {@link Like} that is a "contains" criterion 
	 */
	public static <V extends CharSequence> Like<V> contains() {
		return new Like<>(true, true);
	}
	
	private final boolean leadingStar;
	private final boolean endingStar;
	
	/**
	 * Constructor for "startsWith" and "endsWith" operator without value for now : must be set later with
	 * {@link super#setValue(Object)}
	 * 
	 * @param leadingStar true to add a leading generic '%' character
	 * @param endingStar true to add a ending generic '%' character
	 */
	public Like(boolean leadingStar, boolean endingStar) {
		this.leadingStar = leadingStar;
		this.endingStar = endingStar;
	}
	
	/**
	 * Basic constructor
	 * @param value something that looks like a String, may contain '%' characters
	 */
	public Like(V value) {
		this(value, false, false);
	}
	
	/**
	 * Constructor for "startsWith" and "endsWith" operator
	 * @param value something that looks like a String, may contain '%' characters
	 * @param leadingStar true to add a leading generic '%' character
	 * @param endingStar true to add a ending generic '%' character
	 */
	public Like(V value, boolean leadingStar, boolean endingStar) {
		super(value);
		this.leadingStar = leadingStar;
		this.endingStar = endingStar;
	}
	
	public BiOperandOperator<CharSequence> ignoringCase() {
		LowerCase<CharSequence> lowerCase = new LowerCase<>();
		Like<LowerCase<CharSequence>> charSequenceLike = new Like<>(lowerCase, this.leadingStar, this.endingStar);
		return new BiOperandOperator<CharSequence>() {
			@Override
			public List<Object> asRawCriterion(Object selectable) {
				return Arrays.asList(new LowerCase<>(selectable), charSequenceLike);
			}
			
			@Override
			public void setValue(CharSequence value) {
				lowerCase.setValue(value);
			}
		};
	}
	
	public boolean withLeadingStar() {
		return leadingStar;
	}
	
	public boolean withEndingStar() {
		return endingStar;
	}
}
