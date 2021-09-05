package org.gama.stalactite.sql.binder;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class DerbyTypeMapping extends DefaultTypeMapping {

    public DerbyTypeMapping() {
        super();
		// to prevent syntax error while creating columns : Derby needs varchar length
		put(String.class, "varchar(255)");
	}
}