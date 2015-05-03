package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.model.Phone;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel> extends MetaModel<O> {
	
	public MetaString<MetaPhone> number = new MetaString<>(field(Phone.class, "number"));
	
	public MetaPhone() {
	}
	
	public MetaPhone(AbstractMemberDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		number.setOwner(this);
	}
	
	public MetaString<?> getNumber() {
		return new MetaString<>(method(Phone.class, "getNumber"), this);
	}
	
	public MetaString<MetaPhone> getNumber2() {
		return new MetaString<MetaPhone>(getNumber().getDescription(), this);
	}
}
