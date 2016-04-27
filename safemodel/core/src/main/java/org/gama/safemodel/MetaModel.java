package org.gama.safemodel;

import org.gama.lang.exception.Exceptions;
import org.gama.safemodel.description.ContainerDescription;

import java.lang.reflect.Field;

/**
 * @author Guillaume Mary
 */
public class MetaModel<O extends MetaModel, D extends ContainerDescription> {
	
	private D description;
	
	private O owner;
	
	private Object memberParameter;
	
	public MetaModel() {
	}
	
	public MetaModel(D description) {
		this.description = description;
	}
	
	public MetaModel(D description, O owner) {
		this.description = description;
		this.owner = owner;
	}
	
	/**
	 * Facility method to apply this instance as owner of all its MetaModel fields. Usually called in constructors.
	 * <Strong>Use with caution with inheritance</Strong> because of partial initialization process: subclass fields are null
	 * during the super constructor phase, hence each class of the hierarchy must call {@link #fixFieldsOwner()} to fullfill
	 * its own fields. So parent fields will be "owned" several times (but with the same value).
	 */
	protected void fixFieldsOwner() {
		for (Field field : this.getClass().getDeclaredFields()) {
			if (MetaModel.class.isAssignableFrom(field.getType())) {
				try {
					MetaModel<MetaModel, D> o = (MetaModel) field.get(this);
					// NB: o can be null if an instance calls fixFieldsOwner() before its subclass part is totally initialized.
					// This can occur with inheritance with a super constructor calling fixFieldsOwner()
					if (o != null) {
						o.setOwner(this);
					}
				} catch (IllegalAccessException e) {
					Exceptions.throwAsRuntimeException(e);
				}
			}
		}
	}
	
	public D getDescription() {
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
