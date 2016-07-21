package org.gama.lang.bean;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

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
public class InterfaceIteratorTest {
	
	@DataProvider
	public static Object[][] testNextMethodsData() throws NoSuchFieldException {
		return new Object[][] {
				// test for no direct annotation
				{ Object.class, Arrays.asList() },
				// test for direct annotation
				{ String.class, Arrays.asList(Serializable.class, Comparable.class, CharSequence.class) },
				// test for far inherited annotation
				{ RuntimeException.class, Arrays.asList(Serializable.class) },
				{ ByteArrayInputStream.class, Arrays.asList(Closeable.class, AutoCloseable.class) },
				// test for many annotations by inheritance
				{ StringBuffer.class, Arrays.asList(Serializable.class, CharSequence.class, Appendable.class, CharSequence.class) },
				{ ArrayList.class, Arrays.asList(
						List.class, RandomAccess.class, Cloneable.class, Serializable.class,	// directly on ArrayList
						Collection.class, Iterable.class,	// from List of previous interface list
						List.class, Collection.class, Iterable.class,	// from AbstractList, parent of ArrayList, then List's interaces
						Collection.class, Iterable.class	// from AbstractCollection, parent of AbstractList
				) },
		};
	}
	
	@Test
	@UseDataProvider("testNextMethodsData")
	public void testNextMethods(Class clazz, List<Class> expectedInterfaces) throws Exception {
		InterfaceIterator testInstance = new InterfaceIterator(clazz);
		assertEquals(expectedInterfaces, Iterables.visit(testInstance, new Iterables.ForEach<Class, Class>() {
			
			@Override
			public Class visit(Class interfazz) {
				return interfazz;
			}
		}));
	}
}