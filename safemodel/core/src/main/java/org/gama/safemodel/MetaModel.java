package org.gama.safemodel;

import java.lang.reflect.Field;

import org.gama.lang.bean.FieldIterator;
import org.gama.lang.exception.Exceptions;
import org.gama.safemodel.description.ContainerDescription;

/**
 * @author Guillaume Mary
 */
public class MetaModel<O extends MetaModel, D extends ContainerDescription> {
	
	private O owner;
	
	private D description;
	
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
		FieldIterator fieldIterator = new FieldIterator(getClass());
		while(fieldIterator.hasNext()) {
			Field field = fieldIterator.next();
			if (MetaModel.class.isAssignableFrom(field.getType())) {
				try {
					MetaModel o = (MetaModel) field.get(this);
					// NB: o can be null if an instance calls fixFieldsOwner() before that its subclass part is totally initialized.
					// This can occur with inheritance with a super constructor calling fixFieldsOwner()
					if (o != null) {
						o.setOwner(this);
					}
				} catch (IllegalAccessException e) {
					throw Exceptions.asRuntimeException(e);
				}
			}
		}
	}
	
	public O getOwner() {
		return owner;
	}
	
	public void setOwner(O owner) {
		this.owner = owner;
	}
	
	public D getDescription() {
		return description;
	}
	
	public Object getMemberParameter() {
		return memberParameter;
	}
	
	public void setParameter(Object memberParameter) {
		this.memberParameter = memberParameter;
	}
	
}
