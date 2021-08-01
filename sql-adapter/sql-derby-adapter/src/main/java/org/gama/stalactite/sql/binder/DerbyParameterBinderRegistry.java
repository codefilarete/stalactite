package org.gama.stalactite.sql.binder;

import java.io.InputStream;
import java.sql.Blob;

/**
 * @author Guillaume Mary
 */
public class DerbyParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		register(InputStream.class, DerbyParameterBinders.BINARYSTREAM_BINDER);
		register(byte[].class, DerbyParameterBinders.BYTES_BINDER);
		register(Blob.class, DerbyParameterBinders.BLOB_BINDER);
	}
}
