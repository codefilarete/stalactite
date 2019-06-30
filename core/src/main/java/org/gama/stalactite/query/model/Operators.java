package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.OperatorBuilder;
import org.gama.stalactite.query.model.operand.Between;
import org.gama.stalactite.query.model.operand.Count;
import org.gama.stalactite.query.model.operand.Equals;
import org.gama.stalactite.query.model.operand.Greater;
import org.gama.stalactite.query.model.operand.In;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.gama.stalactite.query.model.operand.Lower;
import org.gama.stalactite.query.model.operand.Max;
import org.gama.stalactite.query.model.operand.Min;
import org.gama.stalactite.query.model.operand.Sum;

/**
 * General contract for operators such as <code>in, like, =, <, >, ... </code>.
 * Value of the operator is intentionnally left vague (Object), except for String operation, because some operators prefer {@link Column}, while
 * others prefers {@link Comparable}.
 * 
 * Static methods should be used to ease a fluent write of queries.
 * 
 * @author Guillaume Mary
 */
// Left abstract with package-private constructor so no one else but classes of this package can extend it because only OperatorBuilder know all of them
public abstract class Operators {
	
	public static <O> Equals<O> eq(O value) {
		return new Equals<>(value);
	}
	
	public static <I extends AbstractRelationalOperator> I not(I operator) {
		operator.setNot();
		return operator;
	}
	
	/**
	 * Shortcut to <code>new Lower(value)</code> to ease a fluent write of queries for "lower than" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link Lower}
	 */
	public static Lower lt(Object value) {
		return new Lower(value);
	}
	
	/**
	 * Shortcut to <code>new Lower(value, true)</code> to ease a fluent write of queries for "lower than equals" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link Lower} with equals checking
	 */
	public static Lower lteq(Object value) {
		return new Lower(value, true);
	}
	
	/**
	 * Shortcut to <code>new Greater(value)</code> to ease a fluent write of queries for "greater than" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link Greater}
	 */
	public static <O> Greater<O>  gt(O value) {
		return new Greater<>(value);
	}
	
	/**
	 * Shortcut to <code>new Greater(value, true)</code> to ease a fluent write of queries for "greater than equals" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link Greater} with equals checking
	 */
	public static <O> Greater<O> gteq(O value) {
		return new Greater<>(value, true);
	}
	
	/**
	 * Shortcut to <code>new Between(value1, value2)</code> to ease a fluent write of queries for "between" comparisons
	 * @param value1 a value, null accepted, transformed to "is null" by {@link OperatorBuilder}) if both values are
	 * @param value2 a value, null accepted, transformed to "is null" by {@link OperatorBuilder}) if both values are
	 * @return a new instance of {@link Between} with equals checking
	 */
	public static <O> Between<O> between(O value1, O value2) {
		return new Between<>(value1, value2);
	}
	
	/**
	 * Shortcut to <code>new In(value)</code> to ease a fluent write of queries for "in" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link In}
	 */
	public static <O> In<O> in(Iterable<O> value) {
		return new In<>(value);
	}
	
	/**
	 * Shortcut to <code>new In(value)</code> to ease a fluent write of queries for "in" comparisons.
	 * Note that this signature won't transform null values to "is null" by {@link OperatorBuilder}), prefers {@link #in(Iterable)} for it.
	 * 
	 * @param value a value, null accepted <b>but won't be transformed</b> to "is null" by {@link OperatorBuilder})
	 * @return a new instance of {@link In}
	 * @see #in(Iterable)
	 */
	public static <O> In<O> in(O ... value) {
		return new In<>(value);
	}
	
	/**
	 * Shortcut to <code>new IsNull()</code> to ease a fluent write of queries for "is null" comparisons
	 * @return a new instance of {@link IsNull}
	 */
	public static IsNull isNull() {
		return new IsNull();
	}
	
	/**
	 * Shortcut to <code>not(new IsNull())</code> to ease a fluent write of queries for "is not null" comparisons
	 * @return a new instance, negative form, of {@link IsNull}
	 */
	public static IsNull isNotNull() {
		return not(isNull());
	}
	
	/**
	 * Shortcut to <code>new Like(value)</code> to ease a fluent write of queries for "like" comparisons
	 * @return a new instance of {@link Like}
	 */
	public static Like like(CharSequence value) {
		return new Like(value);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, true)</code> to ease a fluent write of queries for "contains" comparisons
	 * @return a new instance of {@link Like}
	 */
	public static Like contains(CharSequence value) {
		return new Like(value, true, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, false, true)</code> to ease a fluent write of queries for "startsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	public static Like startsWith(CharSequence value) {
		return new Like(value, false, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, false)</code> to ease a fluent write of queries for "endsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	public static Like endsWith(CharSequence value) {
		return new Like(value, true, false);
	}
	
	/**
	 * Shortcut to <code>new Sum(column)</code> to ease a fluent write of queries for "sum" operation
	 * @return a new instance of {@link Sum}
	 */
	public static <N extends Number> Sum<N> sum(Column<?, N> column) {
		return new Sum<>(column);
	}
	
	/**
	 * Shortcut to <code>new Count(column)</code> to ease a fluent write of queries for "count" operation
	 * @return a new instance of {@link Count}
	 */
	public static Count count(Column column) {
		return new Count(column);
	}
	
	/**
	 * Shortcut to <code>new Min(column)</code> to ease a fluent write of queries for "min" operation
	 * @return a new instance of {@link Min}
	 */
	public static Min min(Column column) {
		return new Min(column);
	}
	
	/**
	 * Shortcut to <code>new Max(column)</code> to ease a fluent write of queries for "max" operation
	 * @return a new instance of {@link Max}
	 */
	public static Max max(Column column) {
		return new Max(column);
	}
	
}
