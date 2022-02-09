package org.codefilarete.stalactite.sql.ddl;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;

/**
 * A default mapping between Java types and SQL type names.
 * This is only used for column types when generating DDL (in upper modules). One can easily overwrite types. 
 * 
 * 
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
		put(Boolean.class, "boolean");
		put(Boolean.TYPE, "boolean");
		put(Double.class, "double");
		put(Double.TYPE, "double");
		put(Float.class, "float");
		put(Float.TYPE, "float");
		put(BigDecimal.class, "decimal(10,4)");
		put(Long.class, "bigint");
		put(Long.TYPE, "bigint");
		put(Integer.class, "integer");
		put(Integer.TYPE, "integer");
		put(Byte.class, "integer");
		put(Byte.TYPE, "integer");
		put(byte[].class, "blob");
		put(Blob.class, "blob");
		put(InputStream.class, "blob");
		put(Timestamp.class, "timestamp");
		put(Date.class, "timestamp");
		put(java.sql.Date.class, "timestamp");
		put(LocalDate.class, "timestamp");
		put(LocalDateTime.class, "timestamp");
		put(LocalTime.class, "timestamp");
		put(String.class, "varchar");
		put(String.class, 16383, "varchar($l)");
		// 35 chars because the largest timezone found is "America/Argentina/ComodRivadavia" (with ZoneId.getAvailableZoneIds())
		put(ZoneId.class, "varchar(35)");
		// necessary entry for Enum, "integer" because Enum are stored by their ordinal by default, see ParameterBinderRegistry.lookupForBinder(Class)
		put(Enum.class, "integer");
		put(UUID.class, "varchar(36)");	// 36 because it UUID length as String
		put(Path.class, "varchar");
		put(Path.class, Integer.MAX_VALUE, "varchar($l)");
		put(File.class, "varchar");
		put(File.class, Integer.MAX_VALUE, "varchar($l)");
	}
}
