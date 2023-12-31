package org.codefilarete.stalactite.sql.statement.binder;

import java.io.File;
import java.nio.file.Path;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class DerbyTypeMapping extends DefaultTypeMapping {
	
	public DerbyTypeMapping() {
		super();
		// to prevent syntax error while creating columns : Derby needs varchar length
		put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, "varchar($l)", Integer.MAX_VALUE);
		put(File.class, "varchar(255)");
		put(File.class, "varchar($l)", Integer.MAX_VALUE);
	}
}