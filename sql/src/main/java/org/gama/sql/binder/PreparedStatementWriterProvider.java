package org.gama.sql.binder;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface PreparedStatementWriterProvider<K> {
	
	/**
	 * Gives a {@link ParameterBinder} from a key.
	 * Will throw an exception in case of missing {@link ParameterBinder}
	 *
	 * @param key an object for which a {@link ParameterBinder} is expected
	 * @return the {@link ParameterBinder} associated with the key 
	 */
	default PreparedStatementWriter getWriter(K key) {
		PreparedStatementWriter writer = doGetWriter(key);
		if (writer == null) {
			throw new RuntimeException("Binder for " + key + " is not registered");
		}
		return writer;
	}
	
	PreparedStatementWriter doGetWriter(K key);
	
	/**
	 * A simple {@link ParameterBinderProvider} that takes its values from a {@link Map}
	 *
	 * @author Guillaume Mary
	 */
	class PreparedStatementWriterProviderFromMap<ParamType> implements PreparedStatementWriterProvider<ParamType> {
		
		private final Map<ParamType, ? extends PreparedStatementWriter> parameterBinders;
		
		public PreparedStatementWriterProviderFromMap(Map<ParamType, ? extends PreparedStatementWriter> parameterBinders) {
			this.parameterBinders = parameterBinders;
		}
		
		public Map<ParamType, ? extends PreparedStatementWriter> getParameterBinders() {
			return parameterBinders;
		}
		
		@Override
		public PreparedStatementWriter doGetWriter(ParamType key) {
			return parameterBinders.get(key);
		}
		
	}
}
