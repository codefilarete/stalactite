package org.gama.stalactite.persistence.sql;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialect extends Dialect { 
	
	public HSQLDBDialect() {
		super(new HSQLDBTypeMapping());
	}
	
	public static class HSQLDBTypeMapping extends DefaultTypeMapping {
		
		public HSQLDBTypeMapping() {
			super();
			// to prevent "length must be specified in type definition: VARCHAR"
			put(String.class, "varchar(255)");
		}
	}
}
