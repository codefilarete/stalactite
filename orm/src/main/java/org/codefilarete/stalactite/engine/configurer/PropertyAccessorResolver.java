package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.AccessorDefinitionDefiner;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByField;
import org.codefilarete.reflection.MutatorByMember;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Reflections.MemberNotFoundException;
import org.codefilarete.tool.Strings;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Resolver for {@link PropertyAccessor} in context of mapping definition.
 * - The API lets one defines property by getter or setter, but persistence engine requires both to fill entity while
 * loading them, also to get their values to persist them. Meanwhile, those methods may not exist, then a direct access
 * to underlying property ({@link Field}) is required.
 * - Moreover, entity may not follow Bean Naming Convention.
 * This class is meant to deal with all this.
 * 
 * @param <C>
 * @param <O>
 * @author Guillaume Mary
 */
public class PropertyAccessorResolver<C, O> {
	
	private final PropertyMapping<C, O> propertyMapping;
	
	public PropertyAccessorResolver(PropertyMapping<C, O> propertyMapping) {
		this.propertyMapping = propertyMapping;
	}
	
	public ReversibleAccessor<C, O> resolve() {
		Accessor<C, O> accessor = null;
		Mutator<C, O> mutator = null;
		AccessorDefinition accessorDefinition = null;
		if (this.propertyMapping.getGetter() != null) {
			AccessorByMethodReference<C, O> getterAsMethodReferenceAccessor = Accessors.accessorByMethodReference(this.propertyMapping.getGetter());
			accessor = getterAsMethodReferenceAccessor;
			
			MutatorByMember<C, O, ?> propertySetter;
			if (this.propertyMapping.getField() != null) {
				propertySetter = new MutatorByField<>(this.propertyMapping.getField());
				accessorDefinition = new AccessorDefinition(getterAsMethodReferenceAccessor.getDeclaringClass(), getterAsMethodReferenceAccessor.getMethodName(), getterAsMethodReferenceAccessor.getPropertyType());
			} else {
				propertySetter = resolveMutator(getterAsMethodReferenceAccessor);
			}
			mutator = propertySetter;
		} else if (this.propertyMapping.getSetter() != null) {
			MutatorByMethodReference<C, O> setterAsMethodReferenceMutator = Accessors.mutatorByMethodReference(this.propertyMapping.getSetter());
			mutator = setterAsMethodReferenceMutator;
			AccessorByMember<C, O, ?> propertyGetter;
			if (this.propertyMapping.getField() != null) {
				propertyGetter = new AccessorByField<>(this.propertyMapping.getField());
				accessorDefinition = new AccessorDefinition(setterAsMethodReferenceMutator.getDeclaringClass(), setterAsMethodReferenceMutator.getMethodName(), setterAsMethodReferenceMutator.getPropertyType());
			} else {
				propertyGetter = resolveAccessor(setterAsMethodReferenceMutator);
			}
			accessor  = propertyGetter;
		}
		if (accessorDefinition != null) {
			return new DefinedPropertyAccessor<>(accessor, mutator, accessorDefinition);
		} else {
			return new PropertyAccessor<>(accessor, mutator);
		}
	}
	
	private MutatorByMember<C, O, ?> resolveMutator(AccessorByMethodReference<C, O> getterAsMethodReferenceAccessor) {
		AccessorDefinition accessorDefinition;
		try {
			accessorDefinition = AccessorDefinition.giveDefinition(getterAsMethodReferenceAccessor);
		} catch (MemberNotFoundException memberNotFoundException) {
			// the getter doesn't follow Java Bean Naming Convention and user didn't give us a field name to set the property
			// we can't go further, so we throw the exception
			throw new MappingConfigurationException("Can't find a property matching getter name "
					+ getterAsMethodReferenceAccessor.getMethodName() + ", setter can't be deduced," +
					" provide a field name to fix it", memberNotFoundException);
		}
		
		String capitalizedProperty = Strings.capitalize(accessorDefinition.getName());
		String methodPrefix = boolean.class.equals(accessorDefinition.getMemberType()) || Boolean.class.equals(accessorDefinition.getMemberType())
				? "is"
				: "set";
		Method method = Reflections.findMethod(accessorDefinition.getDeclaringClass(), methodPrefix + capitalizedProperty, accessorDefinition.getMemberType());
		if (method != null) {
			return new MutatorByMethod<>(method);
		} else {
			// Note that getField will throw an exception if field doesn't exist which shouldn't be the case since AccessorDefinition ensures it
			return new MutatorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), accessorDefinition.getName()));
		}
	}
	
	private AccessorByMember<C, O, ?> resolveAccessor(MutatorByMethodReference<C, O> setterAsMethodReferenceMutator) {
		AccessorDefinition accessorDefinition;
		try {
			accessorDefinition = AccessorDefinition.giveDefinition(setterAsMethodReferenceMutator);
		} catch (MemberNotFoundException memberNotFoundException) {
			// the getter doesn't follow Java Bean Naming Convention and user didn't give us a field name to set the property
			// we can't go further, so we throw the exception
			throw new MappingConfigurationException("Can't find a property matching setter name "
					+ setterAsMethodReferenceMutator.getMethodName() + ", getter can't be deduced," +
					" provide a field name to fix it", memberNotFoundException);
		}
		String capitalizedProperty = Strings.capitalize(accessorDefinition.getName());
		String methodPrefix = boolean.class.equals(accessorDefinition.getMemberType()) || Boolean.class.equals(accessorDefinition.getMemberType())
				? "is"
				: "get";
		Method method = Reflections.findMethod(accessorDefinition.getDeclaringClass(), methodPrefix + capitalizedProperty);
		if (method != null) {
			return new AccessorByMethod<>(method);
		} else {
			// Note that getField will throw an exception if field doesn't exist which shouldn't be the case since AccessorDefinition ensures it
			return new AccessorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), accessorDefinition.getName()));
		}
	}
	
	public interface PropertyMapping<C, O> {
		
		SerializableFunction<C, O> getGetter();
		
		SerializableBiConsumer<C, O> getSetter();
		
		Field getField();
	}
	
	private static class DefinedPropertyAccessor<C, T> extends PropertyAccessor<C, T> implements AccessorDefinitionDefiner<C> {
		
		private final AccessorDefinition accessorDefinition;
		
		public DefinedPropertyAccessor(Accessor<C, T> accessor, Mutator<C, T> mutator, AccessorDefinition accessorDefinition) {
			super(accessor, mutator);
			this.accessorDefinition = accessorDefinition;
		}
		
		@Override
		public AccessorDefinition asAccessorDefinition() {
			return accessorDefinition;
		}
		
		@Override
		public Accessor<C, T> toAccessor() {
			return this;
		}
		
		@Override
		public Mutator<C, T> toMutator() {
			return this;
		}
	}
}
