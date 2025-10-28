package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorChain;
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
		
		AccessorChain<Toto, String> accessor = AccessorChain.fromMethodReferences(Toto::getTata, Tata::getWonderfulProperty);
		assertThat(ColumnNamingStrategy.DEFAULT.giveName(AccessorDefinition.giveDefinition(accessor))).isEqualTo("tata_wonderfulProperty");
	}
	
	private static class Toto {
		
		private int name;
		
		private Tata tata;
		
		public int getName() {
			return name;
		}
		
		public void setName(int name) {
			this.name = name;
		}
		
		public Tata getTata() {
			return tata;
		}
		
		public void setTata(Tata tata) {
			this.tata = tata;
		}
	}
	
	private static class Tata {
		
		private String wonderfulProperty;
		
		public String getWonderfulProperty() {
			return wonderfulProperty;
		}
		
		public void setName(String name) {
			this.wonderfulProperty = name;
		}
	}
}