package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class BeanRelationFixerTest {
	
	@Test
	public void testOf_oneToOne() {
		BeanRelationFixer<DummyTarget, String> testInstance = BeanRelationFixer.of(DummyTarget::setProp1);
		DummyTarget target = new DummyTarget();
		String input = "toto";
		testInstance.apply(target, input);
		assertEquals(input, target.getProp1());
	}
	
	@Test
	public void testOf_oneToMany() {
		BeanRelationFixer<DummyTarget, Integer> testInstance = BeanRelationFixer.of(DummyTarget::setProp2, DummyTarget::getProp2, ArrayList::new);
		DummyTarget target = new DummyTarget();
		testInstance.apply(target, 2);
		testInstance.apply(target, 5);
		assertEquals(Arrays.asList(2, 5), target.getProp2());
	}
	
	private static class DummyTarget {
		private String prop1;
		private List<Integer> prop2;
		
		public String getProp1() {
			return prop1;
		}
		
		public void setProp1(String prop1) {
			this.prop1 = prop1;
		}
		
		public List<Integer> getProp2() {
			return prop2;
		}
		
		public void setProp2(List<Integer> prop2) {
			this.prop2 = prop2;
		}
	}
	
}