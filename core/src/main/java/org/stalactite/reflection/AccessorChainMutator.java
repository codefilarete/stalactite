package org.stalactite.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class AccessorChainMutator<C, X, T> extends AccessorChain<C, X> implements IMutator<C, T> {
	
	public static <C, T> AccessorChainMutator<C, Object, T> toAccessorChainMutator(List<IAccessor> accessors) {
		IAccessor lastAccessor = Iterables.last(accessors);
		IMutator<Object, T> lastMutator = getMutator(lastAccessor);
		return new AccessorChainMutator<>(Collections.cutTail(accessors), lastMutator);
	}
	
	// TODO: shouldn't be static: implements with onXXXX
	public static <C, T> IMutator<C, T> getMutator(IAccessor<C, T> accessor) {
		if (accessor instanceof AccessorByField) {
			Field field = ((AccessorByField) accessor).getGetter();
			MutatorByMethod<C, T> mutatorByMethod = Accessors.mutatorByMethod(field);
			if (mutatorByMethod == null) {
				// no method found => field access is used
				return new MutatorByField<>(field);
			} else {
				return mutatorByMethod;
			}
		} else if (accessor instanceof AccessorByMethod) {
			if (accessor instanceof ListAccessor) {
				return new ListMutator(((ListAccessor) accessor).getIndex());
			}
			AccessorByMethod<C, T> accessorByMethod = (AccessorByMethod<C, T>) accessor;
			Method getter = accessorByMethod.getGetter();
			Class<?> declaringClass = getter.getDeclaringClass();
			String propertyName = Accessors.getPropertyName(getter);
			MutatorByMethod<C, T> mutatorByMethod = Accessors.mutatorByMethod(declaringClass, propertyName);
			if (mutatorByMethod == null) {
				return Accessors.mutatorByField(declaringClass, propertyName);
			} else {
				return mutatorByMethod;
			}
		} else if (accessor instanceof ArrayAccessor) {
			return arrayMutator((ArrayAccessor) accessor);
		} else if (accessor instanceof PropertyAccessor) {
			return ((PropertyAccessor<C, T>) accessor).getMutator();
		} else if (accessor instanceof AccessorChainMutator) {
			return toAccessorChainMutator(((AccessorChainMutator<C, Object, T>) accessor).getAccessors());
		} else {
			throw new IllegalArgumentException("Unknown accessor " + accessor);
		}
	}
	
	public static <C> ArrayMutator<C> arrayMutator(ArrayAccessor<C> lastAccessor) {
		return new ArrayMutator<>(lastAccessor.getIndex());
	}
	
	private final IMutator<X, T> lastMutator;
	
	public AccessorChainMutator(List<IAccessor> accessors, IMutator<X, T> mutator) {
		super(accessors);
		this.lastMutator = mutator;
	}
	
	public AccessorChainMutator(AccessorChain<C, X> accessors, IMutator<X, T> mutator) {
		super(accessors.getAccessors());
		this.lastMutator = mutator;
	}
	
	@Override
	public void set(C c, T t) {
		X target = get(c);
		lastMutator.set(target, t);
	}
}
