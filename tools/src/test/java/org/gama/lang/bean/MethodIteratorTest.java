package org.gama.lang.bean;

import java.lang.reflect.Method;
import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MethodIteratorTest {
	
	@DataProvider
	public static Object[][] testNextMethodsData() throws NoSuchMethodException {
		Method xm1 = X.class.getDeclaredMethod("m1");
		Method ym2 = Y.class.getDeclaredMethod("m2");
		Method zm2 = Z.class.getDeclaredMethod("m2");
		return new Object[][] {
				{ X.class, Arrays.asList(xm1) },
				{ Y.class, Arrays.asList(ym2, xm1) },
				{ Z.class, Arrays.asList(zm2, ym2, xm1) },
				{ NoMethod.class, Arrays.asList(xm1) }
		};
	}
	
	@Test
	@UseDataProvider("testNextMethodsData")
	public void testNextMethods(Class clazz, List<Method> expectedMethods) throws Exception {
		MethodIterator testInstance = new MethodIterator(clazz);
		assertEquals(expectedMethods, Iterables.visit(testInstance, new Iterables.ForEach<Method, Method>() {
			
			@Override
			public Method visit(Method method) {
				return method;
			}
		}));
	}
	
	static class X {
		private void m1() {
		}
	}
	
	static class Y extends X {
		private void m2() {
		}
	}
	
	static class Z extends Y {
		private void m2() {
		}
	}
	
	static class NoMethod extends X {
	}
}