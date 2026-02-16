package org.codefilarete.stalactite.sql.derby.statement.binder;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

import java.io.InputStream;
import java.sql.Blob;
import java.time.LocalDateTime;
import java.time.LocalTime;

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
		// specialized version for LocalDateTime to keep only 6 firsts nanosecond digits, see LOCALDATETIME_BINDER documentation
		register(LocalDateTime.class, DerbyParameterBinders.LOCALDATETIME_BINDER);
		register(LocalTime.class, DerbyParameterBinders.LOCALTIME_BINDER);
	}
}
