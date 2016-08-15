package org.gama.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.lang.bean.Objects;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessor<C, T> implements IAccessor<C, T>, IMutator<C, T> {
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Field field) {
		return new PropertyAccessor<>(new AccessorByField<>(field), new MutatorByField<>(field));
	}
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Class<C> clazz, String propertyName) {
		IAccessor<C, T> propertyGetter = Accessors.accessorByMethod(clazz, propertyName);
		if (propertyGetter == null) {
			propertyGetter = new AccessorByField<>(Reflections.findField(clazz, propertyName));
		}
		IMutator<C, T> propertySetter = Accessors.mutatorByMethod(clazz, propertyName);
		if (propertySetter == null) {
			propertySetter = new MutatorByField<>(Reflections.findField(clazz, propertyName));
		}
		return new PropertyAccessor<>(propertyGetter, propertySetter);
	}
	
	/**
	 * Give an adequate {@link PropertyAccessor} according to the given {@link Member}
	 * @param member a member to be transformed as a {@link PropertyAccessor}
	 * @param <C> the declaring class of the {@link Member}
	 * @param <T> the type of the {@link Member}
	 * @return a new {@link PropertyAccessor} with accessor and mutator alloqing to access to the member
	 */
	public static <C, T> PropertyAccessor<C, T> of(Member member) {
		if (member instanceof Field) {
			return new PropertyAccessor<>(new AccessorByField<>((Field) member), new MutatorByField<>((Field) member));
		} else if (member instanceof Method) {
			// Determining if the method is an accessor or a mutator for given the good arguments to the final PropertyAccessor constructor
			AbstractReflector<Object> reflector = Accessors.onFieldWrapperType((Method) member,
					() -> new AccessorByMethod<>((Method) member),
					() -> new MutatorByMethod<>((Method) member),
					() -> new AccessorByMethod<>((Method) member));
			IAccessor<C, T> accessor;
			IMutator<C, T> mutator;
			if (reflector instanceof IAccessor) {
				accessor = (AccessorByMethod<C, T>) reflector;
				mutator = accessor.toMutator();
			} else if (reflector instanceof IMutator) {
				mutator = (MutatorByMethod<C, T>) reflector;
				accessor = mutator.toAccessor();
			} else {
				// unreachable because preceding ifs check all conditions
				throw new IllegalArgumentException("Member cannot be determined as a getter or a setter : " + member);
			}
			return new PropertyAccessor<>(accessor, mutator);
		} else {
			throw new IllegalArgumentException("Member cannot be used as an accessor : " + member);
		}
	}
	
	
	private final IAccessor<C, T> accessor;
	private final IMutator<C, T> mutator;
	
	public PropertyAccessor(IAccessor<C, T> accessor, IMutator<C, T> mutator) {
		this.accessor = accessor;
		this.mutator = mutator;
	}
	
	public IAccessor<C, T> getAccessor() {
		return accessor;
	}
	
	public IMutator<C, T> getMutator() {
		return mutator;
	}
	
	@Override
	public T get(C c) {
		return this.accessor.get(c);
	}
	
	public void set(C c, T t) {
		this.mutator.set(c, t);
	}
	
	@Override
	public IAccessor<C, T> toAccessor() {
		return getAccessor();
	}
	
	@Override
	public IMutator<C, T> toMutator() {
		return getMutator();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (!(obj instanceof PropertyAccessor)) {
			return super.equals(obj);
		} else {
			return Objects.equalsWithNull(this.getAccessor(), ((PropertyAccessor) obj).getAccessor())
					&& Objects.equalsWithNull(this.getMutator(), ((PropertyAccessor) obj).getMutator());
		}
	}
	
	@Override
	public int hashCode() {
		// Implementation based on both accessor and mutator. Accessor is taken first but it doesn't matter
		return 31 * getAccessor().hashCode() + getMutator().hashCode();
	}
}
