package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaMember;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;
import org.gama.safemodel.model.Phone;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaString<MetaPhone, FieldDescription<String>> number = new MetaString<>(MetaMember.field(Phone.class, "number", String.class));
	
	public MetaPhone() {
	}
	
	public MetaPhone(M accessor) {
		super(accessor);
		fixFieldsOwner();
//		number.setOwner(this);
	}
	
	public MetaString<MetaPhone, MethodDescription<String>> getNumber() {
		return new MetaString<MetaPhone, MethodDescription<String>>(MetaMember.method(Phone.class, "getNumber", String.class), this);
	}
	
}
