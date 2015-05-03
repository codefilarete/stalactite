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
	
}
