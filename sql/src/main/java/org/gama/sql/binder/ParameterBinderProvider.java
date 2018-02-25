package org.gama.sql.binder;

import java.util.Map;

/**
 * Simple contract for giving a {@link ParameterBinder} thanks to a certain kind of key.
 * 
 * @author Guillaume Mary
 */
public interface ParameterBinderProvider<K> extends PreparedStatementWriterProvider<K> {
	
	/**
	 * Gives a {@link ParameterBinder} from a key.
	 * Will throw an exception in case of missing {@link ParameterBinder}
	 * 
	 * @param key an object for which a {@link ParameterBinder} is expected
	 * @return the {@link ParameterBinder} associated with the key 
	 */
	default ParameterBinder getBinder(K key) {
		ParameterBinder binder = doGetBinder(key);
		if (binder == null) {
			throw new RuntimeException("Binder for " + key + " is not registered");
		}
		return binder;
	}
	
	ParameterBinder doGetBinder(K key);
	
	@Override
	default PreparedStatementWriter doGetWriter(K key) {
		return doGetBinder(key);
	}
	
	/**
	 * Short way of getting a {@link ParameterBinderProvider} from a Map
	 * @param parameterBinders the source of {@link ParameterBinder}
	 * @return a {@link ParameterBinderProvider} backed by the Map
	 */
	static <K> ParameterBinderProvider<K> fromMap(Map<K, ParameterBinder> parameterBinders) {
		return new ParameterBinderProviderFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderProvider} that takes its values from a {@link Map}
	 * 
	 * @author Guillaume Mary
	 */
	class ParameterBinderProviderFromMap<ParamType> implements ParameterBinderProvider<ParamType> {
		
		private final Map<ParamType, ParameterBinder> parameterBinders;
		
		public ParameterBinderProviderFromMap(Map<ParamType, ParameterBinder> parameterBinders) {
			this.parameterBinders = parameterBinders;
		}
		
		public Map<ParamType, ParameterBinder> getParameterBinders() {
			return parameterBinders;
		}
		
		@Override
		public ParameterBinder doGetBinder(ParamType key) {
			return parameterBinders.get(key);
		}
		
	}
}
