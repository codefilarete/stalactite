package org.gama.stalactite.persistence.engine;

import org.gama.lang.Reflections;
import org.gama.reflection.Accessors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
public class JoinColumnNamingStrategyTest {
	
	@Test
	public void testDefaultGetName() {
		Assertions.assertEquals("nameId", JoinColumnNamingStrategy.DEFAULT.giveName(Accessors.forProperty(Toto.class, "name")));
		Assertions.assertEquals("nameId", JoinColumnNamingStrategy.DEFAULT.giveName(Accessors.of(Reflections.findMethod(Toto.class, "getName"))));
	}
	
	private static class Toto {
		
		private int name;
		
		public int getName() {
			return name;
		}
	}
}