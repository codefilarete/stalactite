package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ColumnNamingStrategyTest {
	
	@Test
	void defaultGiveName() {
		assertThat(ColumnNamingStrategy.DEFAULT.giveName(new AccessorDefinition(Toto.class, "setName", int.class))).isEqualTo("name");
		assertThat(ColumnNamingStrategy.DEFAULT.giveName(new AccessorDefinition(Toto.class, "getName", String.class))).isEqualTo("name");
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