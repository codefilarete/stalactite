package org.codefilarete.stalactite.sql.result;

import java.util.function.Function;

import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class SimpleBeanCacheTest {
	
	@Test
	void testComputeIfAbsent() {
		SimpleBeanCache testInstance = new SimpleBeanCache();
		ModifiableInt factoryCallCounter = new ModifiableInt();
		Function<Integer, String> capturingFactory = key -> {
			factoryCallCounter.increment();
			return "hello " + key;
		};
		String s = testInstance.computeIfAbsent(String.class, 1, capturingFactory);
		assertThat(s).isEqualTo("hello 1");
		
		// a second call with same key should hit the cache (no factory invocation) and give same result
		s = testInstance.computeIfAbsent(String.class, 1, capturingFactory);
		assertThat(s).isEqualTo("hello 1");
		assertThat(factoryCallCounter.getValue()).isEqualTo(1);
		
		s = testInstance.computeIfAbsent(String.class, 2, capturingFactory);
		assertThat(s).isEqualTo("hello 2");
		assertThat(factoryCallCounter.getValue()).isEqualTo(2);
	}
	
	@Test
	void testComputeIfAbsent_ArrayAsKeySafety() {
		SimpleBeanCache testInstance = new SimpleBeanCache();
		ModifiableInt factoryCallCounter = new ModifiableInt();
		Function<Object[], String> capturingFactory = key -> {
			factoryCallCounter.increment();
			return "hello";
		};
		String s = testInstance.computeIfAbsent(String.class, new Object[] { 1, 2 }, capturingFactory);
		// just to be sure ...
		assertThat(s).isEqualTo("hello");
		// a second call with same key should hit the cache (no factory invocation) and give same result
		s = testInstance.computeIfAbsent(String.class, new Object[] { 1, 2 }, capturingFactory);
		assertThat(s).isEqualTo("hello");
		assertThat(factoryCallCounter.getValue()).isEqualTo(1);
	}
	
}