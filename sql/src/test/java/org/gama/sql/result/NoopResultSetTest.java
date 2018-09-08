package org.gama.sql.result;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.gama.lang.Reflections;
import org.gama.lang.bean.MethodIterator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
				Object result;
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
					result = method.invoke(testInstance, args);
					methodCount++;
				} catch (ReflectiveOperationException | IllegalArgumentException e) {
					throw new RuntimeException("Error executing " + Reflections.toString(method), e);
				}
				// invokation result must be a default value
				assertEquals(Reflections.PRIMITIVE_DEFAULT_VALUES.get(method.getReturnType()), result);
			}
		}
		// checking that iteration over methods really worked
		assertEquals(191, methodCount);
	}
	
}