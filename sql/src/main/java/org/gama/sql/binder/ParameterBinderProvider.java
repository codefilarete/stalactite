package org.gama.sql.binder;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface ParameterBinderProvider<K> {
	
	ParameterBinder getBinder(K key);
	
	/**
	 * Short way of getting a {@link ParameterBinderProvider} from a Map
	 * @param parameterBinders the source of {@link ParameterBinder}
	 * @return a {@link ParameterBinderProvider} backed by the Map
	 */
	static <K> ParameterBinderProvider<K> fromMap(Map<K, ParameterBinder> parameterBinders) {
		return new ParameterBinderProviderFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderIndex} that takes its values from a {@link Map}
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
		public ParameterBinder getBinder(ParamType key) {
			return parameterBinders.get(key);
		}
		
	}
}
