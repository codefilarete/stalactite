package org.stalactite.lang.bean.safemodel;

import org.stalactite.lang.bean.safemodel.model.Address;

/**
 * @author Guillaume Mary
 */
public class MetaAddress<O extends MetaModel> extends MetaModel<O> {
	
	public MetaCity<MetaAddress> city = new MetaCity<>(newDescription(Address.class, "city"));
	public MetaPhone<MetaAddress> phones = new MetaPhone<>(newDescription(Address.class, "phones"));
	
	public MetaAddress() {
	}
	
	public MetaAddress(FieldDescription accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.city.setOwner(this);
//		this.phones.setOwner(this);
	}
	
	public MetaPhone<MetaModel> phones(int i) {
		ListGetMethod accessor = new ListGetMethod(i);
		MetaPhone<MetaModel> metaPhone = new MetaPhone<>(accessor);
		metaPhone.setOwner(phones);
		return metaPhone;
	}
}
