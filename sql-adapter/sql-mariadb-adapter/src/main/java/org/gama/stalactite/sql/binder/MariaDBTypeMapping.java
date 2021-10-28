package org.gama.stalactite.sql.binder;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class MariaDBTypeMapping extends DefaultTypeMapping {
	
	public MariaDBTypeMapping() {
		super();
		put(Integer.class, "int");
		put(Integer.TYPE, "int");
		put(Date.class, "timestamp null");    // null allows nullable in MariaDB, else current time is inserted by default
		// Tweaking types that has nanoseconds stored as TIMESTAMP:
		// - null allows nullable in MariaDB, else current time is inserted by default
		// - precision of 6 because by default MariaDB stores no digit, this allows to comply with SQL-92 and be homogeneous with other databases
		put(LocalDateTime.class, "timestamp(6) null");
		put(Timestamp.class, "timestamp(6) null");
		put(LocalDate.class, "date");
		put(java.sql.Date.class, "date");
		put(String.class, "varchar(255)");
	}
}
