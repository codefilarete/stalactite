package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.result.Accumulators.AccumulatorSupport;

/**
 * Contract to build result of the {@link WholeResultSetTransformer#transformAll(ResultSet, Accumulator)}.
 * Can be seen as a light version of Java Collector, here are main differences :
 * - since its usage is aimed at {@link ResultSet} consumption, there's no need to take parallel iteration into account
 * - it's not expected to address very wide usages as Stream would require
 * 
 * A set of pre-defined {@link Accumulator}s is defined in the {@link Accumulators} class.
 *
 * @param <T> the type of elements to be collected
 * @param <S> the type of the seed that collects elements
 * @param <R> the type of the result
 * @author Guillaume Mary
 * @see Accumulators
 */
public interface Accumulator<T, S, R> {
	
	/**
	 * Should return a factory that build the "collector" that will be given to accumulator.
	 */
	Supplier<S> supplier();
	
	/**
	 * Should return the accumulator that consumes the "collector" and each bean found on row.
	 * The result of this method is invoked for each row found in the {@link ResultSet}.
	 * Input may be an instance that was already previously consumed if row key matches an already consumed one.
	 */
	BiConsumer<S, T> aggregator();
	
	/**
	 * Made to return a function that eventually rework initial seed and return the final result.
	 */
	Function<S, R> finisher();
	
	default <RR> Accumulator<T, S, RR> andThen(Function<R, RR> nextFunction) {
		return new AccumulatorSupport<>(supplier(), aggregator(), finisher().andThen(nextFunction));
	}
	
	/**
	 * Iterates over given input and, foreach element, calls {@link #aggregator()} with element and seed (provided by
	 * {@link #supplier()}), and finally, invokes {@link #finisher()} for returned result arrangement.
	 * 
	 * @param input elements to be iterated over
	 * @return result of above described process
	 */
	default R collect(Iterable<T> input) {
		S seed = supplier().get();
		BiConsumer<S, T> aggregator = aggregator();
		input.forEach(c -> aggregator.accept(seed, c));
		Function<S, R> finisher = finisher();
		return finisher.apply(seed);
	}
}
