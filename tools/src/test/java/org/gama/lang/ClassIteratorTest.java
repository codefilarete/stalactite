package org.gama.lang;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.NoSuchElementException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.Reflections.ClassIterator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClassIteratorTest {
	
	public static final String NEXT_METHODS_DATA = "testNextMethodsData";
	
	@DataProvider(name = NEXT_METHODS_DATA)
	private Object[][] testNextMethodsData() {
		return new Object[][] {
				{ X.class, Arrays.asList((Class) X.class) },
				{ Y.class, Arrays.asList((Class) Y.class, X.class) }
		};
	}
	
	@Test(dataProvider = NEXT_METHODS_DATA)
	public void testNextMethods(Class clazz, List<Class> expectedClasses) throws Exception {
		ClassIterator testInstance = new ClassIterator(clazz);
		assertTrue(testInstance.hasNext());
		for (Class expectedClass : expectedClasses) {
			assertEquals(testInstance.next(), expectedClass);
		}
		assertFalse(testInstance.hasNext());
	}
	
	@Test
	public void testNextMethods_stopClass() throws Exception {
		ClassIterator testInstance = new ClassIterator(Z.class, X.class);
		assertTrue(testInstance.hasNext());
		for (Class expectedClass : Arrays.asList((Class) Z.class, Y.class)) {
			assertEquals(testInstance.next(), expectedClass);
		}
		assertFalse(testInstance.hasNext());
	}
	
	@Test(expectedExceptions = NoSuchElementException.class)
	public void testHasNext_Exception() throws Exception {
		ClassIterator testInstance = new ClassIterator(X.class, X.class);
		testInstance.next();
	}
	
	static class X { }
	
	static class Y extends X { }
	
	static class Z extends Y { }
}