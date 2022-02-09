package binder;

import org.codefilarete.stalactite.sql.binder.ParameterBinderRegistry;

/**
 * @author Guillaume Mary
 */
public class H2ParameterBinderRegistry extends ParameterBinderRegistry {
	
	@Override
	protected void registerParameterBinders() {
		super.registerParameterBinders();
		// nothing to overwrite yet
	}
}
