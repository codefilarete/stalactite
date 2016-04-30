package org.gama.safemodel.metamodel;

import java.util.Collection;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.ListElementAccessDescription;
import org.gama.safemodel.model.Address;
import org.gama.safemodel.model.City;

import static org.gama.safemodel.description.FieldDescription.*;

/**
 * @author Guillaume Mary
 */
public class MetaAddress<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaCity<MetaAddress, FieldDescription<City>> city = new MetaCity<>(field(Address.class, "city", City.class));
	public MetaPhone<MetaAddress, FieldDescription<Collection>> phones = new MetaPhone<>(field(Address.class, "phones", Collection.class));
	
	public MetaAddress() {
	}
	
	public MetaAddress(M accessor) {
		super(accessor);
		fixFieldsOwner();
	}
	
	public MetaPhone<MetaModel, ListElementAccessDescription> phones(int i) {
		ListElementAccessDescription accessor = new ListElementAccessDescription();
		MetaPhone<MetaModel, ListElementAccessDescription> metaPhone = new MetaPhone<>(accessor);
		metaPhone.setOwner(phones);
		metaPhone.setParameter(i);
		return metaPhone;
	}
}
