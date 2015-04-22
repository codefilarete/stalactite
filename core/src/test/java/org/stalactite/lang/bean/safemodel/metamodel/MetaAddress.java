package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.ListGetMethod;
import org.stalactite.lang.bean.safemodel.MetaModel;
import org.stalactite.lang.bean.safemodel.model.Address;

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
		ListGetMethod accessor = new ListGetMethod(i);
		MetaPhone<MetaModel> metaPhone = new MetaPhone<>(accessor);
		metaPhone.setOwner(phones);
		return metaPhone;
	}
}
