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
	
	public MetaModel<?> getNumber() {
		return new MetaModel<>(method(Phone.class, "getNumber"), this);
	}
	
	public MetaString<MetaPhone> getNumber2() {
		return new MetaString<MetaPhone>(getNumber().getDescription(), this);
	}
}
