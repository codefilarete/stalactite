package org.stalactite.benchmark;

import org.stalactite.persistence.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public class HSQLBDDialect extends Dialect { 
	
	public HSQLBDDialect() {
		super(new DefaultTypeMapping() {{
			// pour éviter "length must be specified in type definition: VARCHAR"
			put(String.class, "varchar(255)");
		}});
	}
}
