package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.sql.MySQLDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;

/**
 * @author Guillaume Mary
 */
public class PersistenceMapperTest {
	
	protected static class Toto {
		
		private String name;
		
		public Toto() {
		}
		
		public String getName() {
			return name;
		}
	}
	
	@Test
	public void testPOC() {
		Table toto = new Table("Toto");
		PersistenceMapper.with(Toto.class, toto)
			.map(Toto::getName, toto.new Column("tata", String.class).primaryKey())
			.forDialect(new MySQLDialect());
		
	}
	
}
