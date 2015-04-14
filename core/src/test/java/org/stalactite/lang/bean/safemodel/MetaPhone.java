package org.stalactite.lang.bean.safemodel;

import org.stalactite.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel> extends MetaModel<O> {
	
	MetaModel<MetaPhone> number = new MetaModel<>(getDeclaredField(Phone.class, "number"));
	
	public MetaPhone() {
	}
	
	public MetaPhone(PropertyAccessor accessor) {
		super(accessor);
		fixFieldsOwner();
//		number.setOwner(this);
	}
}
