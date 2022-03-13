package org.codefilarete.stalactite.sql.statement.binder;

import java.io.InputStream;

/**
 * @author Guillaume Mary
 */
public class HSQLDBParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// overwriting InputStream mapping due to HSQLDB usage variation
		register(InputStream.class, HSQLDBParameterBinders.BINARYSTREAM_BINDER);
	}
}
