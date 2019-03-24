package org.gama.sql.binder;

import java.util.Map;

import org.gama.sql.dml.SQLStatement.BindingException;

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
			throw new BindingException("Binder for " + key + " is not registered");
		}
		return binder;
	}
	
	ParameterBinder doGetBinder(K key);
	
	@Override
	default PreparedStatementWriter doGetWriter(K key) {
		return doGetBinder(key);
	}
	
	/**
	 * A simple {@link ParameterBinderProvider} that takes its values from a {@link Map}
	 * 
	 * @author Guillaume Mary
	 */
	class ParameterBinderProviderFromMap<ParamType, BINDER extends ParameterBinder> implements ParameterBinderProvider<ParamType> {
		
		private final Map<ParamType, BINDER> parameterBinders;
		
		public ParameterBinderProviderFromMap(Map<ParamType, BINDER> parameterBinders) {
			this.parameterBinders = parameterBinders;
		}
		
		public Map<ParamType, BINDER> getParameterBinders() {
			return parameterBinders;
		}
		
		@Override
		public BINDER doGetBinder(ParamType key) {
			return parameterBinders.get(key);
		}
		
	}
}
