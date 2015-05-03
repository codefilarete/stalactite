package org.gama.safemodel.metamodel;

import org.gama.safemodel.FieldDescription;
import org.gama.safemodel.ListElementAccessDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.model.Address;

/**
 * @author Guillaume Mary
 */
public class MetaAddress<O extends MetaModel> extends MetaModel<O> {
	
	public MetaCity<MetaAddress> city = new MetaCity<>(field(Address.class, "city"));
	public MetaPhone<MetaAddress> phones = new MetaPhone<>(field(Address.class, "phones"));
	
	public MetaAddress() {
	}
	
	public MetaAddress(FieldDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.city.setOwner(this);
//		this.phones.setOwner(this);
	}
	
	public MetaPhone<MetaModel> phones(int i) {
		ListElementAccessDescription accessor = new ListElementAccessDescription();
		MetaPhone<MetaModel> metaPhone = new MetaPhone<>(accessor);
		metaPhone.setOwner(phones);
		metaPhone.setParameter(i);
		return metaPhone;
	}
}
