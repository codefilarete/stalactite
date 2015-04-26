package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.ListElementAccessDefinition;
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
		ListElementAccessDefinition accessor = new ListElementAccessDefinition();
		MetaPhone<MetaModel> metaPhone = new MetaPhone<>(accessor);
		metaPhone.setOwner(phones);
		metaPhone.setMemberParameter(i);
		return metaPhone;
	}
}
