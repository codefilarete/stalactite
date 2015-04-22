package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.MetaModel;
import org.stalactite.lang.bean.safemodel.model.Phone;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel> extends MetaModel<O> {
	
	public MetaModel<MetaPhone> number = new MetaModel<>(field(Phone.class, "number"));
	
	public MetaPhone() {
	}
	
	public MetaPhone(AbstractMemberDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		number.setOwner(this);
	}
	
	public MetaModel<MetaPhone> getNumber() {
		return new MetaModel<MetaPhone>(method(Phone.class, "getNumber"), this);
	}
}
