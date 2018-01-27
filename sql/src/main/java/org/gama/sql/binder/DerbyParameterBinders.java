package org.gama.sql.binder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.gama.lang.Nullable;
import org.gama.lang.io.IOs;

import static org.gama.sql.DefaultPreparedStatementWriters.BINARYSTREAM_WRITER;

/**
 * @author Guillaume Mary
 */
public final class DerbyParameterBinders {
	
	/**
	 * Must make a copy of the read binary stream from ResultSet, else later reading fails with "java.io.IOException: The object is already closed",
	 * even it the connection is still open and the ResultSet hasn't changed of row. Looks weird.
	 * Looks like https://issues.apache.org/jira/browse/DERBY-6341 but I don't get where I'm wrong (don't see where I read twice the ResultSet).
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>((resultSet, columnName) ->
			Nullable.nullable(resultSet.getBinaryStream(columnName)).applyThrowing(inputStream -> {
				try {
					return IOs.toByteArrayInputStream(inputStream);
				} catch (IOException e) {
					throw new SQLException(e);
				}
			}).get(), BINARYSTREAM_WRITER);
	
	private DerbyParameterBinders() {
	}
}
