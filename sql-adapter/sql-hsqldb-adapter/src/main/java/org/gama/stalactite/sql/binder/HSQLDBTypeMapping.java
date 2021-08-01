package org.gama.stalactite.sql.binder;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class HSQLDBTypeMapping extends DefaultTypeMapping {

    public HSQLDBTypeMapping() {
        super();
        // to prevent "length must be specified in type definition: VARCHAR"
        put(String.class, "varchar(255)");
    }
}