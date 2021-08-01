package org.gama.stalactite.sql.binder;

import java.sql.Blob;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// overwriting Blob mapping due to PostgreSQL way of handling Blobs
		register(Blob.class, PostgreSQLParameterBinders.BLOB_BINDER);
	}
}
