package org.gama.stalactite.sql.result;

import org.gama.lang.function.ThrowingFunction;
import org.gama.lang.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class SimpleBeanCacheTest {
	
	@Test
	void testComputeIfAbsent() {
		SimpleBeanCache testInstance = new SimpleBeanCache();
		ModifiableInt factoryCallCounter = new ModifiableInt();
		ThrowingFunction<Integer, String, RuntimeException> capturingFactory = key -> {
			factoryCallCounter.increment();
			return "hello " + key;
		};
		String s = testInstance.computeIfAbsent(String.class, 1, capturingFactory);
		assertEquals("hello 1", s);
		
		// a second call with same key should hit the cache (no factory invokation) and give same result
		s = testInstance.computeIfAbsent(String.class, 1, capturingFactory);
		assertEquals("hello 1", s);
		assertEquals(1, factoryCallCounter.getValue());
		
		s = testInstance.computeIfAbsent(String.class, 2, capturingFactory);
		assertEquals("hello 2", s);
		assertEquals(2, factoryCallCounter.getValue());
	}
	
	@Test
	void testComputeIfAbsent_ArrayAsKeySafety() {
		SimpleBeanCache testInstance = new SimpleBeanCache();
		ModifiableInt factoryCallCounter = new ModifiableInt();
		ThrowingFunction<Object[], String, RuntimeException> capturingFactory = key -> {
			factoryCallCounter.increment();
			return "hello";
		};
		String s = testInstance.computeIfAbsent(String.class, new Object[] { 1, 2 }, capturingFactory);
		// just to be sure ...
		assertEquals("hello", s);
		// a second call with same key should hit the cache (no factory invokation) and give same result
		s = testInstance.computeIfAbsent(String.class, new Object[] { 1, 2 }, capturingFactory);
		assertEquals("hello", s);
		assertEquals(1, factoryCallCounter.getValue());
	}
	
}