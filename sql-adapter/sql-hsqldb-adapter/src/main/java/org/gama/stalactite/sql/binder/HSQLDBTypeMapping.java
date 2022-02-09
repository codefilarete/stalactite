package org.codefilarete.stalactite.sql.binder;

import java.io.File;
import java.nio.file.Path;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class HSQLDBTypeMapping extends DefaultTypeMapping {

    public HSQLDBTypeMapping() {
        super();
        // to prevent "length must be specified in type definition: VARCHAR"
        put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, Integer.MAX_VALUE, "varchar($l)");
		put(File.class, "varchar(255)");
		put(File.class, Integer.MAX_VALUE, "varchar($l)");
    }
}