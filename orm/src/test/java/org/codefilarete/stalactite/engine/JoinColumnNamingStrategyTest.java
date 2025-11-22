package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoinColumnNamingStrategyTest {
	
	@Test
	void defaultGiveName() {
		Table table = new Table("DummyTable");
		Column idColumn = table.addColumn("id", int.class);
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "name", int.class), idColumn)).isEqualTo("nameId");
		assertThat(JoinColumnNamingStrategy.JOIN_DEFAULT.giveName(new AccessorDefinition(Toto.class, "isbn", String.class), idColumn)).isEqualTo("isbnId");
	}
	
	private static class Toto {
		
		private int name;
		
		private String isbn;
		
		public int getName() {
			return name;
		}
		
		public void setName(int name) {
			this.name = name;
		}
		
		public String getIsbn() {
			return isbn;
		}
		
		public void setIsbn(String isbn) {
			this.isbn = isbn;
		}
	}
	
}