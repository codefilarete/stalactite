package org.codefilarete.stalactite.sql.statement.binder;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class MySQLTypeMapping extends DefaultTypeMapping {
	
	public MySQLTypeMapping() {
		super();
		put(Integer.class, "int");
		put(Integer.TYPE, "int");
		put(Date.class, "timestamp null");    // null allows nullable in MySQL, else current time is inserted by default
		// Tweaking types that have nanoseconds stored as TIMESTAMP:
		// - null allows nullable in MySQL, else current time is inserted by default
		// - precision of 6 because by default MySQL stores no digit, this allows to comply with SQL-92 and be homogeneous with other databases
		put(LocalDateTime.class, "timestamp(6) null");
		put(LocalTime.class, "timestamp(6) null");
		put(Timestamp.class, "timestamp(6) null");
		put(LocalDate.class, "date");
		put(java.sql.Date.class, "date");
		// to prevent syntax error while creating schema: MySQL requires varchar size
		put(String.class, "varchar(255)");
		put(Path.class, "varchar(255)");
		put(Path.class, Integer.MAX_VALUE, "varchar($l)");
		put(File.class, "varchar(255)");
		put(File.class, Integer.MAX_VALUE, "varchar($l)");
	}
}
