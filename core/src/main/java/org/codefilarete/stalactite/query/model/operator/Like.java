package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValueWrapper.SQLFunctionWrapper;

/**
 * Represents a like comparison.
 * Options to create a "startsWith" and "endsWith" are provided.
 * 
 * @author Guillaume Mary
 */
public class Like<V extends CharSequence> extends UnitaryOperator<V> {
	
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
	
	/**
	 * Constructor for contains operator with a function as argument (for lower/upper case for example)
	 * @param value the function that composing this like
	 */
	public Like(SQLFunction<V> value) {
		this(value, false, false);
	}
	
	/**
	 * Constructor for "startsWith" and "endsWith" operator
	 * @param value something that looks like a String, may contain '%' characters
	 * @param leadingStar true to add a leading generic '%' character
	 * @param endingStar true to add a ending generic '%' character
	 */
	public Like(SQLFunction<V> value, boolean leadingStar, boolean endingStar) {
		super(new SQLFunctionWrapper<>(value));
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
