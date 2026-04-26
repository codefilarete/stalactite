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
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByField;
import org.codefilarete.reflection.MutatorByMember;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Reflections.MemberNotFoundException;
import org.codefilarete.tool.Strings;

/**
 * Resolver for {@link ReadWriteAccessPoint} in context of mapping definition.
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
	
	private final AccessPointCoordinates<C, O> accessPointCoordinates;
	
	public PropertyAccessorResolver(AccessPointCoordinates<C, O> accessPointCoordinates) {
		this.accessPointCoordinates = accessPointCoordinates;
	}
	
	public ReadWritePropertyAccessPoint<C, O> resolve() {
		PropertyAccessor<C, O> accessor = null;
		PropertyMutator<C, O> mutator = null;
		AccessorDefinition accessorDefinition = null;
		if (this.accessPointCoordinates.getGetter() != null) {
			AccessorByMethodReference<C, O> getterAsMethodReferenceAccessor = Accessors.accessorByMethodReference(this.accessPointCoordinates.getGetter());
			accessor = getterAsMethodReferenceAccessor;
			
			MutatorByMember<C, O, ?> propertySetter;
			if (this.accessPointCoordinates.getField() != null) {
				propertySetter = new MutatorByField<>(this.accessPointCoordinates.getField());
				accessorDefinition = new AccessorDefinition(getterAsMethodReferenceAccessor.getDeclaringClass(), getterAsMethodReferenceAccessor.getMethodName(), getterAsMethodReferenceAccessor.getPropertyType());
			} else {
				propertySetter = resolveMutator(getterAsMethodReferenceAccessor);
			}
			mutator = propertySetter;
		} else if (this.accessPointCoordinates.getSetter() != null) {
			MutatorByMethodReference<C, O> setterAsMethodReferenceMutator = Accessors.mutatorByMethodReference(this.accessPointCoordinates.getSetter());
			mutator = setterAsMethodReferenceMutator;
			AccessorByMember<C, O, ?> propertyGetter;
			if (this.accessPointCoordinates.getField() != null) {
				propertyGetter = new AccessorByField<>(this.accessPointCoordinates.getField());
				accessorDefinition = new AccessorDefinition(setterAsMethodReferenceMutator.getDeclaringClass(), setterAsMethodReferenceMutator.getMethodName(), setterAsMethodReferenceMutator.getPropertyType());
			} else {
				propertyGetter = resolveAccessor(setterAsMethodReferenceMutator);
			}
			accessor  = propertyGetter;
		}
		if (accessorDefinition != null) {
			return new DefinedReadWriteAccessPoint<>(accessor, mutator, accessorDefinition);
		} else {
			return new DefaultReadWritePropertyAccessPoint<>(accessor, mutator);
		}
	}
	
	private MutatorByMember<C, O, ?> resolveMutator(AccessorByMethodReference<C, O> getterAsMethodReferenceAccessor) {
		AccessorDefinition accessorDefinition = getterAsMethodReferenceAccessor.asAccessorDefinition();
		String propertyName;
		try {
			propertyName = Reflections.propertyName(getterAsMethodReferenceAccessor.getMethodName());
		} catch (MemberNotFoundException memberNotFoundException) {
			// the getter doesn't follow Java Bean Naming Convention and user didn't give us a field name to set the property
			// we can't go further, so we throw the exception
			throw new MappingConfigurationException("Can't find a property matching getter name "
					+ getterAsMethodReferenceAccessor.getMethodName() + ", setter can't be deduced," +
					" provide a field name to fix it", memberNotFoundException);
		}
		Method method = Reflections.findMethod(accessorDefinition.getDeclaringClass(), "set" + Strings.capitalize(propertyName), accessorDefinition.getMemberType());
		if (method != null) {
			return new MutatorByMethod<>(method);
		} else {
			// Note that getField will throw an exception if field doesn't exist which shouldn't be the case since AccessorDefinition ensures it
			return new MutatorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), propertyName));
		}
	}
	
	private AccessorByMember<C, O, ?> resolveAccessor(MutatorByMethodReference<C, O> setterAsMethodReferenceMutator) {
		AccessorDefinition accessorDefinition = setterAsMethodReferenceMutator.asAccessorDefinition();
		String propertyName;
		try {
			propertyName = Reflections.propertyName(setterAsMethodReferenceMutator.getMethodName());
		} catch (MemberNotFoundException memberNotFoundException) {
			// the getter doesn't follow Java Bean Naming Convention and user didn't give us a field name to set the property
			// we can't go further, so we throw the exception
			throw new MappingConfigurationException("Can't find a property matching setter name "
					+ setterAsMethodReferenceMutator.getMethodName() + ", getter can't be deduced," +
					" provide a field name to fix it", memberNotFoundException);
		}
		String methodPrefix = boolean.class.equals(accessorDefinition.getMemberType()) || Boolean.class.equals(accessorDefinition.getMemberType())
				? "is"
				: "get";
		Method method = Reflections.findMethod(accessorDefinition.getDeclaringClass(), methodPrefix + Strings.capitalize(propertyName));
		if (method != null) {
			return new AccessorByMethod<>(method);
		} else {
			// Note that getField will throw an exception if field doesn't exist which shouldn't be the case since AccessorDefinition ensures it
			return new AccessorByField<>(Reflections.getField(accessorDefinition.getDeclaringClass(), accessorDefinition.getName()));
		}
	}
	
	public interface AccessPointCoordinates<C, O> {
		
		SerializablePropertyAccessor<C, O> getGetter();
		
		SerializablePropertyMutator<C, O> getSetter();
		
		Field getField();
	}
	
	private static class DefinedReadWriteAccessPoint<C, T> extends DefaultReadWritePropertyAccessPoint<C, T> implements AccessorDefinitionDefiner<C> {
		
		private final AccessorDefinition accessorDefinition;
		
		public DefinedReadWriteAccessPoint(PropertyAccessor<C, T> accessor, PropertyMutator<C, T> mutator, AccessorDefinition accessorDefinition) {
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
