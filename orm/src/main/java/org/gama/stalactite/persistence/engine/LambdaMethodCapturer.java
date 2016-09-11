package org.gama.stalactite.persistence.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.IVisitor;
import org.gama.spy.ByteBuddySpy;

/**
 * A method that allows to get the {@link Method} represented by a method reference like "{@code String::toString}".
 * This is impossible without code enhancement. For this I choosed {@link ByteBuddySpy} ... because it was there !
 * But it could be done with CGLib or whatever API.
 * 
 * Inspired from <a href="http://stackoverflow.com/questions/9864300/how-to-get-method-object-in-java-without-using-method-string-names/22745127">a stackoverflow answer</a>
 * and, overall, <a href="http://benjiweber.co.uk/blog/2013/12/28/typesafe-database-interaction-with-java-8/">the solution</a>
 * 
 * @author Guillaume Mary
 * @see <a href="http://benjiweber.co.uk/blog/2013/12/28/typesafe-database-interaction-with-java-8/">principle explained</a>
 */
public class LambdaMethodCapturer<T> {
	
	public static final Method BICONSUMER_ACCEPT_METHOD = Reflections.findMethod(BiConsumer.class, "accept", Object.class, Object.class);
	private final T targetInstance;
	
	private final ByteBuddySpy<T> spy;
	
	public LambdaMethodCapturer(Class<T> declaringClass) {
		// Code enhancer for creation of a proxy that will support functions invocations
		this.spy = new ByteBuddySpy<>();
		this.targetInstance = Reflections.newInstance(declaringClass);
	}
	
	public Method capture(Function<T, ?> function) {
		// Capturer of method calls
		Method[] result = new Method[1];
		T dummyInstance = this.spy.spy(this.targetInstance, (target, method, args) -> result[0] = method);
		// calling functions for method harvesting
		function.apply(dummyInstance);
		return result[0];
	}
	
	public <I> Method capture(BiConsumer<T, I> function) {
		// Capturer of method calls
		Method[] result = new Method[1];
		T dummyInstance = this.spy.spy(this.targetInstance, (target, method, args) -> result[0] = method);
		// calling functions for method harvesting
		try {
			function.accept(dummyInstance, null);
		} catch (NullPointerException e) {
			// The floowing is horrible: we're here surely because of the null argument previously passed, that means the method reference
			// (BiConsumer) take a primitive parameter. We can't call function.accept(dummyInstance, ..) with another type than "I" (doesn't compile)
			// so we hide it thru a reflective invokation hence it's not checked by compilation.
			// Then ... we have to test all primitive types because we can't know which one it is (that's the purpose of capture(..) !) 
			Iterables.visit(Arrays.asList(0, 0L, 0F, 0D, Boolean.FALSE, (byte) 0), new ClassCastExceptionRetryer<>(function, dummyInstance));
		}
		if (result[0] == null) {
			throw new UnsupportedOperationException("Method for lambda expression can't be determined : " + function);
		}
		return result[0];
	}
	
	private static <T, I> void quietInvokation(BiConsumer<T, I> function, T dummyInstance, Object input) throws Throwable {
		try {
			BICONSUMER_ACCEPT_METHOD.invoke(function, dummyInstance, input);
		} catch (IllegalAccessException e) {
			// should no happen
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			// may happen
			throw e.getCause();
		}
	}
	
	/**
	 * {@link IVisitor} that will still run some code while its throws a {@link ClassCastException} 
	 * @param <T>
	 */
	private static class ClassCastExceptionRetryer<T> implements IVisitor<Object, Object> {
		
		private final BiConsumer<T, ?> function;
		private final T dummyInstance;
		private ClassCastException capturedException;
		
		public ClassCastExceptionRetryer(BiConsumer<T, ?> function, T dummyInstance) {
			this.function = function;
			this.dummyInstance = dummyInstance;
		}
		
		@Override
		public Object visit(Object input) {
			capturedException = null;
			try {
				quietInvokation(function, dummyInstance, input);
			} catch (Throwable t) {
				if (t instanceof ClassCastException) {
					capturedException = (ClassCastException) t;
				} else {
					// wrap error in a RuntimeException else requires many signature change whereas the code behind function has already been executed
					// by the caller during the "function.accept(dummyInstance, null);" phase
					throw new RuntimeException(t);
				}
			}
			return null;
		}
		
		@Override
		public boolean pursue() {
			// we stop when there's no more ClassCastException
			return capturedException != null;
		}
	}
}
