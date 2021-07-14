package org.gama.stalactite.persistence.engine;

import org.gama.reflection.AccessorDefinition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ColumnNamingStrategyTest {
	
	@Test
	public void testDefaultGetName() {
		assertThat(ColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "setName", int.class))).isEqualTo("nameId");
		assertThat(ColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "getName", String.class))).isEqualTo("nameId");
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