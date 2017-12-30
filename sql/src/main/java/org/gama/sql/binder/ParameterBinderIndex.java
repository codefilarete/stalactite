package org.gama.sql.binder;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A contract for a {@link ParameterBinder} index.
 * The difference with {@link ParameterBinderProvider} is that an index can be queried for its whole keys and content, hence permiting optimization
 * for "massive" operation such as iterating over all the {@link ParameterBinder}.
 * 
 * @author Guillaume Mary
 */
public interface ParameterBinderIndex<K> extends ParameterBinderProvider<K> {
	
	/**
	 * @return all available keys
	 */
	Set<K> keys();
	
	/**
	 * Aims at being used for iterating over all keys and values instead of calling {@link #getBinder(Object)} for each element of {@link #keys()}
	 * @return all the key + value pairs of this index
	 */
	Set<Entry<K, ParameterBinder>> all();
	
	/**
	 * Short way of getting a {@link ParameterBinderIndex} from a Map
	 * @param parameterBinders the source of {@link ParameterBinder}
	 * @return a {@link ParameterBinderIndex} backed by the Map
	 */
	static <K> ParameterBinderIndex<K> fromMap(Map<K, ParameterBinder> parameterBinders) {
		return new ParameterBinderIndexFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderIndex} that takes its values from a {@link Map}
	 * 
	 * @author Guillaume Mary
	 */
	class ParameterBinderIndexFromMap<ParamType> extends ParameterBinderProviderFromMap<ParamType> implements ParameterBinderIndex<ParamType> {
		
		public ParameterBinderIndexFromMap(Map<ParamType, ParameterBinder> parameterBinders) {
			super(parameterBinders);
		}
		
		@Override
		public Set<ParamType> keys() {
			return getParameterBinders().keySet();
		}
		
		@Override
		public Set<Entry<ParamType, ParameterBinder>> all() {
			return getParameterBinders().entrySet();
		}
	}
}
