package org.codefilarete.stalactite.sql.statement.binder;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

import static org.codefilarete.stalactite.sql.ddl.Size.length;

/**
 * @author Guillaume Mary
 */
public class OracleTypeMapping extends DefaultTypeMapping {
	
	public OracleTypeMapping() {
		super();
		put(BigDecimal.class, "float");
		put(Long.class, "integer");
		put(Long.TYPE, "integer");
		put(BigInteger.class, "integer");
		put(Double.class, "float");
		put(Double.TYPE, "float");
		// Oracle doesn't support varchar without size
		put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, "varchar($l)", length(255));
		put(File.class, "varchar(255)");
		put(File.class, "varchar($l)", length(255));
		// Oracle supports natively ZonedDateTime and OffsetDateTime storage through type "timestamp with time zone"
		put(ZonedDateTime.class, "timestamp with time zone");
		put(OffsetDateTime.class, "timestamp with time zone");
	}
}
