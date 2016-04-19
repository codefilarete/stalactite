package org.gama.lang;

import org.gama.lang.Reflections.ClassIterator;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
		assertEquals(expectedClasses, Iterables.copy(testInstance));
	}
	
	@Test
	public void testNextMethods_stopClass() throws Exception {
		ClassIterator testInstance = new ClassIterator(Z.class, X.class);
		assertEquals(Arrays.asList((Class) Z.class, Y.class), Iterables.copy(testInstance));
	}
	
	@Test
	public void testHasNext_false() throws Exception {
		ClassIterator testInstance = new ClassIterator(X.class, X.class);
		assertFalse(testInstance.hasNext());
	}
	
	static class X { }
	
	static class Y extends X { }
	
	static class Z extends Y { }
}