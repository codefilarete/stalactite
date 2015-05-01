package org.stalactite.reflection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.stalactite.lang.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class AccessorChain<C, T> implements IAccessor<C, T>{
	
	private final List<IAccessor> accessors;
	
	public AccessorChain() {
		this(new ArrayList<IAccessor>(5));
	}
	
	public AccessorChain(List<IAccessor> accessors) {
		this.accessors = accessors;
	}
	
	public List<IAccessor> getAccessors() {
		return accessors;
	}
	
	public void add(IAccessor accessor) {
		accessors.add(accessor);
	}
	
	public void add(IAccessor ... accessors) {
		add(Arrays.asList(accessors));
	}
	
	public void add(Iterable<IAccessor> accessors) {
		this.accessors.addAll((Collection<IAccessor>) accessors);
	}
	
	@Override
	public T get(C c) {
		Object target = c;
		for (IAccessor accessor : accessors) {
			target = accessor.get(target);
		}
		return (T) target;
	}
}
