package org.gama.sql.binder;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Specialized {@link ParameterBinder}s for HSQLDB.
 * 
 * @author Guillaume Mary
 */
public final class HSQLDBParameterBinders {
	
	/**
	 * Specialization is made by overriding {@link PreparedStatement#setBinaryStream(int, InputStream)} for null values because it fails
	 * with "org.hsqldb.HsqlException: Invalid argument in JDBC call"
	 * HsqlDB 2.3.2
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(ResultSet::getBinaryStream, (p, i, v) -> {
		if (v == null) {
			p.setObject(i, null);
		} else {
			p.setBinaryStream(i, v);
		}
	});
	
	private HSQLDBParameterBinders() {}
}
