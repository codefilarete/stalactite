package org.gama.stalactite.persistence.sql;

import java.time.LocalDateTime;
import java.util.Date;

import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;

/**
 * A default mapping between Java types and SQL type names
 * <br>
 * <table>
 * <thead>
 * <tr><td>Java</td><td>SQL type names</td></trt>
 * </thead>
 * <tr><td>Boolean</td><td>bit</td></trt>
 * <tr><td>Double</td><td>double</td></trt>
 * <tr><td>Float</td><td>float</td></trt>
 * <tr><td>Long</td><td>bigint</td></trt>
 * <tr><td>Integer</td><td>integer</td></trt>
 * <tr><td>Date</td><td>timestamp</td></trt>
 * <tr><td>LocalDateTime</td><td>timestamp</td></trt>
 * <tr><td>String</td><td>varchar</td></trt>
 * <tr><td>String with size</td><td>varchar(size)</td></trt>
 * </table>
 *
 * @author Guillaume Mary
 */
public class DefaultTypeMapping extends JavaTypeToSqlTypeMapping {
	
	public DefaultTypeMapping() {
		super();
		put(Boolean.class, "bit");
		put(Boolean.TYPE, "bit");
		put(Double.class, "double");
		put(Double.TYPE, "double");
		put(Float.class, "float");
		put(Float.TYPE, "float");
		put(Long.class, "bigint");
		put(Long.TYPE, "bigint");
		put(Integer.class, "integer");
		put(Integer.TYPE, "integer");
		put(Date.class, "timestamp");
		put(LocalDateTime.class, "timestamp");
		put(String.class, "varchar");
		put(String.class, 16383, "varchar($l)");
	}
}
