package org.gama.lang;

import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Reflections.ClassIterator;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class ClassIteratorTest {
	
	@DataProvider
	public static Object[][] testNextMethodsData() {
		return new Object[][] {
				{ X.class, Arrays.asList((Class) X.class) },
				{ Y.class, Arrays.asList((Class) Y.class, X.class) }
		};
	}
	
	@Test
	@UseDataProvider("testNextMethodsData")
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