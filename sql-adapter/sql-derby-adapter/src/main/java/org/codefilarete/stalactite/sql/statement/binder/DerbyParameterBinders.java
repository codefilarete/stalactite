package org.codefilarete.stalactite.sql.statement.binder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.io.IOs;

import static org.codefilarete.stalactite.sql.statement.binder.DefaultPreparedStatementWriters.BINARYSTREAM_WRITER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultPreparedStatementWriters.BLOB_WRITER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultPreparedStatementWriters.BYTES_WRITER;

/**
 * @author Guillaume Mary
 */
public final class DerbyParameterBinders {
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BINARYSTREAM_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "java.io.IOException: The object is already closed",
	 * even it the connection is still open and the ResultSet hasn't changed of row. Looks weird.
	 * Looks like https://issues.apache.org/jira/browse/DERBY-6341 but I don't get where I'm wrong (don't see where I read twice the ResultSet).
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBinaryStream(columnName)).mapThrower(inputStream -> {
				try {
					return IOs.toByteArrayInputStream(inputStream);
				} catch (IOException e) {
					throw new SQLException(e);
				}
			}).get(), BINARYSTREAM_WRITER);
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BYTES_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "Stream or LOB value cannot be retrieved more than once".
	 */
	public static final ParameterBinder<byte[]> BYTES_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBytes(columnName)).mapThrower(bytes -> Arrays.copyOf(bytes, bytes.length)).get(), BYTES_WRITER);
	
	/**
	 * Equivalent to {@link DefaultParameterBinders#BLOB_BINDER} but makes a copy of the read binary stream from ResultSet, else later reading fails
	 * with "Stream or LOB value cannot be retrieved more than once".
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBlob(columnName)).mapThrower(blob -> new InMemoryBlobSupport(blob.getBytes(1, (int) blob.length()))).get(), BLOB_WRITER);
	
	/**
	 * Dedicated {@link LocalDateTime} {@link ParameterBinder} for Derby because its TIMESTAMP SQL type stores all 9 nanosecond digits (at least
	 * in 10.5, wasn't the case in some previous versions), therefore it defers from other databases and SQL-92 standard that stores 6 digits by
	 * default. Furthermore, Derby TIMESTAMP can't be configured for precision. So, to keep a homogeneous behavior between databases it was choosen
	 * to store only 6 firsts digits of given {@link LocalDateTime} nanos : this binder does it.
	 */
	public static final ParameterBinder<LocalDateTime> LOCALDATETIME_BINDER = new NullAwareParameterBinder<>(
			new NullAwareResultSetReader<>(DefaultParameterBinders.LOCALDATETIME_BINDER),
			new NullAwarePreparedStatementWriter<>((preparedStatement, valueIndex, localDateTime) -> {
				localDateTime = localDateTime.minusNanos(localDateTime.getNano() % 1000);	// 3 last digits are erased (input is not changed)
				Timestamp timestamp = Timestamp.valueOf(localDateTime);
				preparedStatement.setTimestamp(valueIndex, timestamp);
			}));
	
	/**
	 * Dedicated {@link LocalTime} {@link ParameterBinder} for Derby because its TIMESTAMP SQL type stores all 9 nanosecond digits (at least
	 * in 10.5, wasn't the case in some previous versions), therefore it defers from other databases and SQL-92 standard that stores 6 digits by
	 * default. Furthermore, Derby TIMESTAMP can't be configured for precision. So, to keep a homogeneous behavior between databases it was choosen
	 * to store only 6 firsts digits of given {@link LocalDateTime} nanos : this binder does it.
	 */
	public static final ParameterBinder<LocalTime> LOCALTIME_BINDER = new NullAwareParameterBinder<>(
			new NullAwareResultSetReader<>(DefaultParameterBinders.LOCALTIME_BINDER),
			new NullAwarePreparedStatementWriter<>((preparedStatement, valueIndex, localDateTime) -> {
				localDateTime = localDateTime.minusNanos(localDateTime.getNano() % 1000);	// 3 last digits are erased (input is not changed)
				Timestamp timestamp = Timestamp.valueOf(localDateTime.atDate(LocalTimeBinder.DEFAULT_TIMESTAMP_REFERENCE_DATE));
				preparedStatement.setTimestamp(valueIndex, timestamp);
			}));
	
	private DerbyParameterBinders() {
	}
}
