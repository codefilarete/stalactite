package org.gama.stalactite.sql.binder;

import java.io.File;
import java.nio.file.Path;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class DerbyTypeMapping extends DefaultTypeMapping {
	
	public DerbyTypeMapping() {
		super();
		// to prevent syntax error while creating columns : Derby needs varchar length
		put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, Integer.MAX_VALUE, "varchar($l)");
		put(File.class, "varchar(255)");
		put(File.class, Integer.MAX_VALUE, "varchar($l)");
	}
}