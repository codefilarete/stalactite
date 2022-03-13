package org.codefilarete.stalactite.sql.ddl.structure;

/**
 * @author mary
 */
public class Database {
	
	public class Schema {
		
		private String name;
		
		public Schema(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public Database getDatabase() {
			return Database.this;
		}
	}
}
