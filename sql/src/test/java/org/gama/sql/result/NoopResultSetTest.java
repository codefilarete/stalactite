package org.gama.sql.result;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.gama.lang.Reflections;
import org.gama.lang.bean.MethodIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class NoopResultSetTest {
	
	@Test
	public void testAllMethodsReturnDefaultValue() {
		ResultSet testInstance = new NoopResultSet();
		MethodIterator methodIterator = new MethodIterator(NoopResultSet.class, Object.class);
		Iterable<Method> methods = () -> methodIterator;
		int methodCount = 0;
		for (Method method : methods) {
			if (method.getReturnType() != Void.class) {
				Object invokationResult;
				try {
					// we create default arguments otherwise we get IllegalArgumentException from the JVM at invoke() time
					Object[] args = new Object[method.getParameterCount()];
					Class<?>[] parameterTypes = method.getParameterTypes();
					for (int i = 0; i < parameterTypes.length; i++) {
						Class arg = parameterTypes[i];
						if (arg.isArray()) {
							args[i] = Array.newInstance(arg.getComponentType(), 0);
						} else {
							args[i] = Reflections.PRIMITIVE_DEFAULT_VALUES.getOrDefault(arg, null /* default value for any non-primitive Object */);
						}
					}
					invokationResult = method.invoke(testInstance, args);
					methodCount++;
				} catch (ReflectiveOperationException | IllegalArgumentException e) {
					throw new RuntimeException("Error executing " + Reflections.toString(method), e);
				}
				// invokation result must be a default value
				if (method.getReturnType().isArray()) {
					assertTrue(invokationResult.getClass().isArray());
					assertEquals(method.getReturnType().getComponentType(), invokationResult.getClass().getComponentType());
				} else {
					assertEquals(Reflections.PRIMITIVE_DEFAULT_VALUES.get(method.getReturnType()), invokationResult);
				}
			}
		}
		// checking that iteration over methods really worked
		assertEquals(191, methodCount);
	}
	
}