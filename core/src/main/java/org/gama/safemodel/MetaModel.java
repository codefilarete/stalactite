package org.gama.safemodel;

import java.lang.reflect.Field;

import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class MetaModel<O extends MetaModel> {
	
	protected static FieldDescription field(Class clazz, String name) {
		return new FieldDescription(clazz, name);
	}
	
	protected static MethodDescription method(Class clazz, String name, Class ... parameterTypes) {
		return new MethodDescription(clazz, name, parameterTypes);
	}
	
	private AbstractMemberDescription description;
	
	private O owner;
	
	private Object memberParameter;
	
	public MetaModel() {
	}
	
	public MetaModel(AbstractMemberDescription description) {
		this.description = description;
	}
	
	public MetaModel(AbstractMemberDescription description, O owner) {
		this.description = description;
		this.owner = owner;
	}
	
	protected void fixFieldsOwner() {
		for (Field field : this.getClass().getDeclaredFields()) {
			if (MetaModel.class.isAssignableFrom(field.getType())) {
				try {
					MetaModel<MetaModel> o = (MetaModel) field.get(this);
					o.setOwner(this);
				} catch (IllegalAccessException e) {
					Exceptions.throwAsRuntimeException(e);
				}
			}
		}
	}
	
	public AbstractMemberDescription getDescription() {
		return description;
	}
	
	public O getOwner() {
		return owner;
	}
	
	public void setOwner(O owner) {
		this.owner = owner;
	}
	
	public Object getMemberParameter() {
		return memberParameter;
	}
	
	public void setParameter(Object memberParameter) {
		this.memberParameter = memberParameter;
	}
	
	public static abstract class AbstractMemberDescription {
		
		private final Class declaringClass;
		
		public AbstractMemberDescription(Class declaringClass) {
			this.declaringClass = declaringClass;
		}
		
		public Class getDeclaringClass() {
			return declaringClass;
		}
	}
	
	public static class ArrayDescription extends AbstractMemberDescription {
		
		public ArrayDescription(Class declaringClass) {
			super(declaringClass);
		}
	}
	
	public static abstract class AbstracNamedtMemberDescription extends AbstractMemberDescription {
		
		private final String name;
		
		public AbstracNamedtMemberDescription(Class declaringClass, String name) {
			super(declaringClass);
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	public static class FieldDescription extends AbstracNamedtMemberDescription {
		
		public FieldDescription(Class declaringClass, String name) {
			super(declaringClass, name);
		}
	}
	
	public static class MethodDescription extends AbstracNamedtMemberDescription {
		
		private final Class[] parameterTypes;
		
		public MethodDescription(Class declaringClass, String name, Class ... parameterTypes) {
			super(declaringClass, name);
			this.parameterTypes = parameterTypes;
		}
		
		public Class[] getParameterTypes() {
			return parameterTypes;
		}
	}
	
	public static class GetterSetterDescription extends MethodDescription {
		
		private final String setterName;
		
		public GetterSetterDescription(Class declaringClass, String getterName, String setterName, Class... parameterTypes) {
			super(declaringClass, getterName, parameterTypes);
			this.setterName = setterName;
		}
		
		public String getGetterName() {
			return getName();
		}
		public String getSetterName() {
			return setterName;
		}
	}
}
