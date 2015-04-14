package org.stalactite.lang.bean.safemodel;

import org.stalactite.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaPerson<O extends MetaModel> extends MetaModel<O> {
	
	MetaAddress<MetaPerson> address = new MetaAddress<>(getDeclaredField(Person.class, "address"));
	
	public MetaPerson() {
	}
	
	public MetaPerson(PropertyAccessor accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.address.setOwner(this);
	}
}
