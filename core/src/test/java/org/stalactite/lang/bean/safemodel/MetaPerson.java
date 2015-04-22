package org.stalactite.lang.bean.safemodel;

import org.stalactite.lang.bean.safemodel.model.Person;

/**
 * @author Guillaume Mary
 */
public class MetaPerson<O extends MetaModel> extends MetaModel<O> {
	
	public MetaAddress<MetaPerson> address = new MetaAddress<>(newDescription(Person.class, "address"));
	
	public MetaPerson() {
	}
	
	public MetaPerson(FieldDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.address.setOwner(this);
	}
}
