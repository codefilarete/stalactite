package org.gama.stalactite.sql.binder;

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
public interface ParameterBinderIndex<K, BINDER extends ParameterBinder> extends ParameterBinderProvider<K>, PreparedStatementWriterIndex<K, BINDER> {
	
	/**
	 * @return all available keys
	 */
	Set<K> keys();
	
	/**
	 * Aims at being used for iterating over all keys and values instead of calling {@link #getBinder(Object)} for each element of {@link #keys()}
	 * @return all the key + value pairs of this index
	 */
	Set<Entry<K, BINDER>> all();
	
	/**
	 * Short way of getting a {@link ParameterBinderIndex} from a Map
	 * @param parameterBinders the source of {@link ParameterBinder}
	 * @return a {@link ParameterBinderIndex} backed by the Map
	 */
	static <K, BINDER extends ParameterBinder> ParameterBinderIndex<K, BINDER> fromMap(Map<K, BINDER> parameterBinders) {
		return new ParameterBinderIndexFromMap<>(parameterBinders);
	}
	
	/**
	 * A simple {@link ParameterBinderIndex} that takes its values from a {@link Map}
	 * 
	 * @author Guillaume Mary
	 */
	class ParameterBinderIndexFromMap<ParamType, BINDER extends ParameterBinder> extends ParameterBinderProviderFromMap<ParamType, BINDER> implements ParameterBinderIndex<ParamType, BINDER> {
		
		public ParameterBinderIndexFromMap(Map<ParamType, BINDER> parameterBinders) {
			super(parameterBinders);
		}
		
		@Override
		public Set<ParamType> keys() {
			return getParameterBinders().keySet();
		}
		
		@Override
		public Set<Entry<ParamType, BINDER>> all() {
			return getParameterBinders().entrySet();
		}
	}
}
