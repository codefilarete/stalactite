package org.gama.sql.binder;

import java.io.InputStream;

/**
 * @author Guillaume Mary
 */
public class DerbyParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// overwriting InputStream mapping due to Derby usage variation
		register(InputStream.class, DerbyParameterBinders.BINARYSTREAM_BINDER);
	}
}
