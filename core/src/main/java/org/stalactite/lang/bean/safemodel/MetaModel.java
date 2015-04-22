package org.stalactite.lang.bean.safemodel;

import java.lang.reflect.Field;

import org.stalactite.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class MetaModel<O extends MetaModel> {
	
	protected static FieldDescription newDescription(Class clazz, String name) {
		return new FieldDescription(clazz, name);
	}
	
	protected static MethodDescription newMethodDescription(Class clazz, String name, Class ... parameters) {
		return new MethodDescription(clazz, name, parameters);
	}
	
	private AbstractMemberDescription description;
	
	private O owner;
	
	public MetaModel() {
	}
	
	public MetaModel(AbstractMemberDescription description) {
		this.description = description;
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
	
	public static abstract class AbstractMemberDescription {
		
		private final Class declaringClass;
		private final String name;
		
		public AbstractMemberDescription(Class declaringClass, String name) {
			this.declaringClass = declaringClass;
			this.name = name;
		}
		
		public Class getDeclaringClass() {
			return declaringClass;
		}
		
		public String getName() {
			return name;
		}
	}
	
	public static class FieldDescription extends AbstractMemberDescription {
		
		public FieldDescription(Class declaringClass, String name) {
			super(declaringClass, name);
		}
	}
	
	public static class MethodDescription extends AbstractMemberDescription {
		
		private final Object[] parameters;
		
		public MethodDescription(Class declaringClass, String name, Object[] parameters) {
			super(declaringClass, name);
			this.parameters = parameters;
		}
		
		public Object[] getParameters() {
			return parameters;
		}
	}
}
