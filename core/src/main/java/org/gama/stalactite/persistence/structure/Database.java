package org.gama.stalactite.persistence.structure;

/**
 * @author mary
 */
public class Database {
	
	public class Schema {
		
		private String name;
		
		public Schema(String name) {
			this.name = name;
		}
		
		public Database getDatabase() {
			return Database.this;
		}
	}
}
