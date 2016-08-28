package org.gama.lang;

import org.gama.lang.trace.IncrementableInt;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Guillaume Mary
 */
public class NullableTest {
	
	@Test
	public void orApply() throws Exception {
		// simple case
		assertEquals("Hello World", Nullable.of("Hello").orApply((o) -> o + " World").get());
		// with null value
		assertNull(Nullable.of(null).orApply((o) -> { fail("this code should not even be invoked"); return o + " World"; }).get());
	}
	
	@Test
	public void orAccept() throws Exception {
		// simple case
		IncrementableInt consumerCallCount = new IncrementableInt();
		Nullable.of("Hello").orAccept((o) -> consumerCallCount.increment());
		assertEquals(1, consumerCallCount.getValue());
		// with null value
		Nullable.of(null).orAccept((o) -> { fail("this code should not even be invoked"); consumerCallCount.increment(); });
	}
	
	@Test
	public void orTest() throws Exception {
		// simple case
		assertTrue(Nullable.of("Hello").orTest((o) -> o.equals("Hello")).get());
		// with null value
		Nullable.of(null).orTest((o) -> { fail("this code should not even be invoked"); return false; });
	}
	
}