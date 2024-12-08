package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.UnvaluedVariable;
import org.codefilarete.stalactite.query.model.operator.Between.Interval;

public class DefaultNamedOperator<T, V> {
	
	public static <T, V> ConditionalOperator<T, V> arg(String name, ConditionalOperator<T, V> delegate, Class<T> type) {
		delegate.setValue(new UnvaluedVariable<>(name, type));
		return delegate;
	}
	
	public static <T> ConditionalOperator<T, T> equalsArgNamed(String name, Class<T> type) {
		return arg(name, new Equals<>(), type);
	}
	
	public static <T> ConditionalOperator<T, Iterable<T>> inArgNamed(String name, Class<T> type) {
		return arg(name, new In<>(), type);
	}
	
	public static <T extends CharSequence> ConditionalOperator<T, T> likeArgNamed(String name, Class<T> type) {
		return arg(name, new Like<>(), type);
	}
	
	public static <T extends CharSequence> ConditionalOperator<T, T> containsArgNamed(String name, Class<T> type) {
		return arg(name, Like.contains(), type);
	}
	
	public static <T extends CharSequence> ConditionalOperator<T, T> startsWithArgNamed(String name, Class<T> type) {
		return arg(name, Like.startsWith(), type);
	}
	
	public static <T extends CharSequence> ConditionalOperator<T, T> endsWithArgNamed(String name, Class<T> type) {
		return arg(name, Like.endsWith(), type);
	}
	
	public static <T> ConditionalOperator<T, T> isLowerThanArgNamed(String name, Class<T> type) {
		return arg(name, new Lesser<>(), type);
	}
	
	public static <T> ConditionalOperator<T, T> isLowerOrEqualsThanArgNamed(String name, Class<T> type) {
		return arg(name, new Lesser<T>().equals(), type);
	}
	
	public static <T> ConditionalOperator<T, T> isGreaterThanArgNamed(String name, Class<T> type) {
		return arg(name, new Greater<T>().equals(), type);
	}
	
	public static <T> ConditionalOperator<T, T> isGreaterOrEqualsThanArgNamed(String name, Class<T> type) {
		return arg(name, new Greater<T>().equals(), type);
	}
	
	public static <T> ConditionalOperator<T, Interval<T>> isBetweenArgNamed(String name, Class<T> type) {
		return arg(name, new Between<>(), type);
	}
}
