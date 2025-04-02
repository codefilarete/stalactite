package org.codefilarete.stalactite.sql.statement.binder;

import java.io.File;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

import static org.codefilarete.stalactite.sql.ddl.Size.length;

/**
 * @author Guillaume Mary
 */
public class HSQLDBTypeMapping extends DefaultTypeMapping {

    public HSQLDBTypeMapping() {
        super();
        // to prevent "length must be specified in type definition: VARCHAR"
        put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, "varchar($l)", length(Integer.MAX_VALUE));
		put(File.class, "varchar(255)");
		put(File.class, "varchar($l)", length(Integer.MAX_VALUE));
		// Oracle supports natively ZonedDateTime and OffsetDateTime storage through type "timestamp with time zone"
		put(ZonedDateTime.class, "timestamp with time zone");
		put(OffsetDateTime.class, "timestamp with time zone");
    }
}