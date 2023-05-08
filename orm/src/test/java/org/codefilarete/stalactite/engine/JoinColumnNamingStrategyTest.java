package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoinColumnNamingStrategyTest {
	
	@Test
	void defaultGiveName() {
		Table table = new Table("DummyTable");
		Column idColumn = table.addColumn("id", int.class);
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "setName", int.class), idColumn)).isEqualTo("nameId");
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "getName", String.class), idColumn)).isEqualTo("nameId");
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