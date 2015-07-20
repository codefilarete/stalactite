package org.gama.lang;

import org.gama.lang.Reflections.ClassIterator;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.PairIterator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
		PairIterator<Class, Class> expectationComparator = new PairIterator.UntilBothIterator<>(expectedClasses.iterator(), testInstance);
		while(expectationComparator.hasNext()) {
			Map.Entry<Class, Class> next = expectationComparator.next();
			assertEquals(next.getKey(), next.getValue());
		}
	}
	
	@Test
	public void testNextMethods_stopClass() throws Exception {
		ClassIterator testInstance = new ClassIterator(Z.class, X.class);
		assertTrue(testInstance.hasNext());
		for (Class expectedClass : Arrays.asList((Class) Z.class, Y.class)) {
			assertEquals(expectedClass, testInstance.next());
		}
		assertFalse(testInstance.hasNext());
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