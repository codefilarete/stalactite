package org.gama.lang;

import java.util.function.Function;

/**
 * @author Guillaume Mary
 */
public class Lambdas {
	
	/**
	 * Run some code before another
	 * @param surrogate the code to execute
	 * @param runnable the code to be executed before
	 * @param <I> input parameter type
	 * @param <O> ouput parameter type
	 * @return the result of surrogate.apply()
	 */
	public static <I, O> Function<I, O> before(Function<I, O> surrogate, Runnable runnable) {
		return i -> {
			runnable.run();
			return surrogate.apply(i);
		};
	}
}
