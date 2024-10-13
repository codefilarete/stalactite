package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.Coalesce;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.Lower;
import org.codefilarete.stalactite.query.model.operator.LowerCase;
import org.codefilarete.stalactite.query.model.operator.Max;
import org.codefilarete.stalactite.query.model.operator.Min;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.query.model.operator.Substring;
import org.codefilarete.stalactite.query.model.operator.Sum;
import org.codefilarete.stalactite.query.model.operator.Trim;
import org.codefilarete.stalactite.query.model.operator.UpperCase;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * General contract for operators such as <code>in, like, =, <, >, ... </code>.
 * Value of the operator is intentionally left vague (Object), except for String operation, because some operators prefer {@link Column}, while
 * others prefers {@link Comparable}.
 * 
 * Static methods should be used to ease a fluent write of queries.
 * 
 * @author Guillaume Mary
 */
public interface Operators {
	
	static <O> Equals<O> eq(O value) {
		return new Equals<>(value);
	}
	
	static <I extends ConditionalOperator> I not(I operator) {
		operator.not();
		return operator;
	}
	
	/**
	 * Shortcut to <code>new Lower(value)</code> to ease a fluent write of queries for "lower than" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link Lower}
	 */
	static <O> Lower<O> lt(O value) {
		return new Lower<>(value);
	}
	
	/**
	 * Shortcut to <code>new Lower(value, true)</code> to ease a fluent write of queries for "lower than equals" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link Lower} with equals checking
	 */
	static <O> Lower<O> lteq(O value) {
		return new Lower<>(value, true);
	}
	
	/**
	 * Shortcut to <code>new Greater(value)</code> to ease a fluent write of queries for "greater than" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link Greater}
	 */
	static <O> Greater<O> gt(O value) {
		return new Greater<>(value);
	}
	
	/**
	 * Shortcut to <code>new Greater(value, true)</code> to ease a fluent write of queries for "greater than equals" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link Greater} with equals checking
	 */
	static <O> Greater<O> gteq(O value) {
		return new Greater<>(value, true);
	}
	
	/**
	 * Shortcut to <code>new Between(value1, value2)</code> to ease a fluent write of queries for "between" comparisons
	 * @param value1 a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder}) if both values are
	 * @param value2 a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder}) if both values are
	 * @return a new instance of {@link Between} with equals checking
	 */
	static <O> Between<O> between(O value1, O value2) {
		return new Between<>(value1, value2);
	}
	
	/**
	 * Shortcut to <code>new In(value)</code> to ease a fluent write of queries for "in" comparisons
	 * @param value a value, null accepted, transformed to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link In}
	 */
	static <O> In<O> in(Iterable<O> value) {
		return new In<>(value);
	}
	
	/**
	 * Shortcut to <code>new In(value)</code> to ease a fluent write of queries for "in" comparisons.
	 * Note that this signature won't transform null values to "is null" by {@link OperatorSQLBuilder}), prefers {@link #in(Iterable)} for it.
	 * 
	 * @param value a value, null accepted <b>but won't be transformed</b> to "is null" by {@link OperatorSQLBuilder})
	 * @return a new instance of {@link In}
	 * @see #in(Iterable)
	 */
	static <O> In<O> in(O ... value) {
		return new In<>(value);
	}
	
	/**
	 * Shortcut to <code>new IsNull()</code> to ease a fluent write of queries for "is null" comparisons
	 * @return a new instance of {@link IsNull}
	 */
	static IsNull isNull() {
		return new IsNull();
	}
	
	/**
	 * Shortcut to <code>not(new IsNull())</code> to ease a fluent write of queries for "is not null" comparisons
	 * @return a new instance, negative form, of {@link IsNull}
	 */
	static IsNull isNotNull() {
		return not(isNull());
	}
	
	/**
	 * Shortcut to <code>new Trim(value)</code> to ease a fluent write of queries for "trim" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Trim<V> trim(V value) {
		return new Trim<>(value);
	}
	
	/**
	 * Shortcut to <code>new Trim(value)</code> to ease a fluent write of queries for "trim" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Trim<Selectable<V>> trim(Selectable<V> value) {
		return new Trim<>(value);
	}
	
	/**
	 * Shortcut to <code>new Trim(value)</code> to ease a fluent write of queries for "trim" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Trim<SQLFunction<?, CharSequence>> trim(SQLFunction<?, CharSequence> value) {
		return new Trim<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "upper" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> UpperCase<V> upperCase(V value) {
		return new UpperCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "upper" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> UpperCase<Selectable<V>> upperCase(Selectable<V> value) {
		return new UpperCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "upper" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> UpperCase<SQLFunction<?, V>> upperCase(SQLFunction<?, V> value) {
		return new UpperCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "lower" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> LowerCase<V> lowerCase(V value) {
		return new LowerCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "lower" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> LowerCase<Selectable<V>> lowerCase(Selectable<V> value) {
		return new LowerCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new UpperCase(value)</code> to ease a fluent write of queries for "lower" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> LowerCase<SQLFunction<?, V>> lowerCase(SQLFunction<?, V> value) {
		return new LowerCase<>(value);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(V value, int from) {
		return new Substring<>(value, from);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from, to)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(V value, int from, int to) {
		return new Substring<>(value, from, to);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(Selectable<V> value, int from) {
		return new Substring<>(value, from);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from, to)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(Selectable<V> value, int from, int to) {
		return new Substring<>(value, from, to);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(SQLFunction<?, V> value, int from) {
		return new Substring<>(value, from);
	}
	
	/**
	 * Shortcut to <code>new Substring(value, from, to)</code> to ease a fluent write of queries for "substring" functions
	 * @return a new instance of {@link Trim}
	 */
	static <V extends CharSequence> Substring<V> substring(SQLFunction<?, V> value, int from, int to) {
		return new Substring<>(value, from, to);
	}
	
	/**
	 * Shortcut to <code>new Like(value)</code> to ease a fluent write of queries for "like" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<V> like(V value) {
		return new Like<>(value);
	}
	
	/**
	 * Shortcut to <code>new Like(value)</code> to ease a fluent write of queries for "like" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<SQLFunction<?, V>> like(SQLFunction<?, V> value) {
		return new Like<>(value);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, true)</code> to ease a fluent write of queries for "contains" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<V> contains(V value) {
		return new Like<>(value, true, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, true)</code> to ease a fluent write of queries for "contains" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<SQLFunction<?, V>> contains(SQLFunction<?, V> value) {
		return new Like<>(value, true, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, false, true)</code> to ease a fluent write of queries for "startsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<V> startsWith(V value) {
		return new Like<>(value, false, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, false, true)</code> to ease a fluent write of queries for "startsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<SQLFunction<?, V>> startsWith(SQLFunction<?, V> value) {
		return new Like<>(value, false, true);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, false)</code> to ease a fluent write of queries for "endsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<V> endsWith(V value) {
		return new Like<>(value, true, false);
	}
	
	/**
	 * Shortcut to <code>new Like(value, true, false)</code> to ease a fluent write of queries for "endsWith" comparisons
	 * @return a new instance of {@link Like}
	 */
	static <V extends CharSequence> Like<SQLFunction<?, V>> endsWith(SQLFunction<?, V> value) {
		return new Like<>(value, true, false);
	}
	
	/**
	 * Shortcut to <code>new Sum(column)</code> to ease a fluent write of queries for "sum" operation
	 * @return a new instance of {@link Sum}
	 */
	static <N extends Number> Sum<N> sum(Selectable<N> column) {
		return new Sum<>(column);
	}
	
	/**
	 * Shortcut to <code>new Count(column)</code> to ease a fluent write of queries for "count" operation
	 * @return a new instance of {@link Count}
	 */
	static Count count(Selectable<?>... columns) {
		return new Count(columns);
	}
	
	/**
	 * Shortcut to <code>new Count(column)</code> to ease a fluent write of queries for "count" operation
	 * @return a new instance of {@link Count}
	 */
	static Count count(Iterable<? extends Selectable<?>> columns) {
		return new Count(columns);
	}
	
	/**
	 * Shortcut to <code>new Min(column)</code> to ease a fluent write of queries for "min" operation
	 * @return a new instance of {@link Min}
	 */
	static <N extends Number> Min<N> min(Selectable<N> column) {
		return new Min<>(column);
	}
	
	/**
	 * Shortcut to <code>new Max(column)</code> to ease a fluent write of queries for "max" operation
	 * @return a new instance of {@link Max}
	 */
	static <N extends Number> Max<N> max(Selectable<N> column) {
		return new Max<>(column);
	}
	
	/**
	 * Shortcut to <code>new Cast(expression, javaType)</code> to ease a fluent write of queries for "cast" operation
	 * @return a new instance of {@link Cast}
	 */
	static <O> Cast<?, O> cast(String expression, Class<O> javaType) {
		return new Cast<>(expression, javaType);
	}
	
	/**
	 * Shortcut to <code>new Cast(expression, javaType)</code> to ease a fluent write of queries for "cast" operation
	 * @return a new instance of {@link Cast}
	 */
	static <C, O> Cast<C, O> cast(Selectable<C> expression, Class<O> javaType) {
		return new Cast<>(expression, javaType);
	}
	
	/**
	 * Shortcut to <code>new Coalesce(column, columns)</code> to ease a fluent write of queries for "coalesce" operation
	 * @return a new instance of {@link Coalesce}
	 */
	static <C> Coalesce<C> coalesce(Selectable<C> column, Selectable<C>... columns) {
		return new Coalesce<>(column, columns);
	}
}
