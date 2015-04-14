package org.stalactite.lang.bean.safemodel;

import org.stalactite.reflection.AccessorForList;
import org.stalactite.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaAddress<O extends MetaModel> extends MetaModel<O> {
	
	MetaCity<MetaAddress> city = new MetaCity<>(getDeclaredField(Address.class, "city"));
	MetaPhone<MetaAddress> phones = new MetaPhone<>(getDeclaredField(Address.class, "phones"));
	
	public MetaAddress() {
	}
	
	public MetaAddress(PropertyAccessor accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.city.setOwner(this);
//		this.phones.setOwner(this);
	}
	
	public MetaPhone<MetaModel> phones(int i) {
		AccessorForList accessor = new AccessorForList();
		accessor.setIndex(i);
		MetaPhone<MetaModel> metaModelMetaPhone = new MetaPhone<>(accessor);
		metaModelMetaPhone.setOwner(phones);
		return metaModelMetaPhone;
	}
}
