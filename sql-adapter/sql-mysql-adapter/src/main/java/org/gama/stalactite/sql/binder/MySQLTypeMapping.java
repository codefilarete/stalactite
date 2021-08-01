package org.gama.stalactite.sql.binder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import org.gama.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class MySQLTypeMapping extends DefaultTypeMapping {

    public MySQLTypeMapping() {
        super();
        put(Integer.class, "int");
        put(Integer.TYPE, "int");
        put(Date.class, "timestamp null");    // null allows nullable in MySQL, else current time is inserted by default
        put(LocalDateTime.class, "timestamp null");
        put(LocalDate.class, "date");
        put(java.sql.Date.class, "date");
        put(String.class, "varchar(255)");
    }
}