package org.stalactite.reflection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
		Object previousTarget;
		Iterator<IAccessor> iterator = accessors.iterator();
		while (iterator.hasNext()) {
			IAccessor accessor = iterator.next();
			previousTarget = target;
			target = accessor.get(target);
			if (target == null && iterator.hasNext()) {
				throwNullPointerException(previousTarget, accessor);
			}
		}
		return (T) target;
	}
	
	public void throwNullPointerException(Object previousTarget, IAccessor accessor) {
		String accessorDescription = accessor.toString();
		String exceptionMessage;
		if (accessor instanceof AccessorByField) {
			exceptionMessage = previousTarget + " has null value on field " + ((AccessorByField) accessor).getGetter().getName();
		} else {
			exceptionMessage = "Call of " + accessorDescription + " on " + previousTarget + " returned null";
		}
		throw new NullPointerException(exceptionMessage);
	}
}
