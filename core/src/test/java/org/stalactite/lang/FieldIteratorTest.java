package org.stalactite.lang;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.stalactite.lang.Reflections.FieldIterator;
import org.stalactite.lang.collection.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldIteratorTest {
	
	public static final String NEXT_METHODS_DATA = "testNextMethodsData";
	
	@DataProvider(name = NEXT_METHODS_DATA)
	private Object[][] testNextMethodsData() {
		return new Object[][] {
				{ X.class, Arrays.asList("f1") },
				{ Y.class, Arrays.asList("f2", "f1") },
				{ Z.class, Arrays.asList("f2", "f2", "f1") }
		};
	}
	
	@Test(dataProvider = NEXT_METHODS_DATA)
	public void testNextMethods(Class clazz, List<String> expectedFields) throws Exception {
		FieldIterator testInstance = new FieldIterator(clazz);
		assertTrue(testInstance.hasNext());
		for (String expectedField : expectedFields) {
			assertEquals(testInstance.next().getName(), expectedField);
		}
		assertFalse(testInstance.hasNext());
	}
	
	static class X { private String f1; }
	
	static class Y extends X { private String f2; }
	
	static class Z extends Y { private String f2; }
}