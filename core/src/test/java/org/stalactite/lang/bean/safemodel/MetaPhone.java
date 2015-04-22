package org.stalactite.lang.bean.safemodel;

import org.stalactite.lang.bean.safemodel.model.Phone;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel> extends MetaModel<O> {
	
	public MetaModel<MetaPhone> number = new MetaModel<>(newDescription(Phone.class, "number"));
	
	public MetaPhone() {
	}
	
	public MetaPhone(AbstractMemberDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		number.setOwner(this);
	}
}
