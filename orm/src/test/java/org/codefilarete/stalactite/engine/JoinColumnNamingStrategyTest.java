package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoinColumnNamingStrategyTest {
	
	@Test
	void defaultGiveName() {
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "setName", int.class), null)).isEqualTo("nameId");
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "getName", String.class), null)).isEqualTo("nameId");
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