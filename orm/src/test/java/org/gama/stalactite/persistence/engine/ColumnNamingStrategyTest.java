package org.gama.stalactite.persistence.engine;

import org.gama.lang.Reflections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
public class ColumnNamingStrategyTest {
	
	@Test
	public void testDefaultGetName() {
		Assertions.assertEquals("nameId", ColumnNamingStrategy.JOIN_DEFAULT.giveName(Reflections.getMethod(Toto.class, "setName", int.class)));
		Assertions.assertEquals("nameId", ColumnNamingStrategy.JOIN_DEFAULT.giveName(Reflections.getMethod(Toto.class, "getName")));
	}
	
	private static class Toto {
		
		private int name;
		
		public int getName() {
			return name;
		}
		
		public void setName(int name) {
			this.name = name;
		}
	}
}