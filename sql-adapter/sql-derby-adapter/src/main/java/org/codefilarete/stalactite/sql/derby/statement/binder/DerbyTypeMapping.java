package org.codefilarete.stalactite.sql.derby.statement.binder;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

import static org.codefilarete.stalactite.sql.ddl.Size.fixedPoint;
import static org.codefilarete.stalactite.sql.ddl.Size.length;

/**
 * @author Guillaume Mary
 */
public class DerbyTypeMapping extends DefaultTypeMapping {
	
	public DerbyTypeMapping() {
		super();
		// to prevent syntax error while creating columns : Derby needs varchar length
		put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, "varchar($l)", length(Integer.MAX_VALUE));
		put(File.class, "varchar(255)");
		put(File.class, "varchar($l)", length(Integer.MAX_VALUE));
		// Derby can only handle BigDecimal with 31 as precision instead of 38 for other databases
		replace(BigDecimal.class, "decimal($p, $s)", fixedPoint(31, 2));
	}
}
