package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;
import org.gama.safemodel.lang.MetaString;
import org.gama.safemodel.model.Phone;

import static org.gama.safemodel.description.FieldDescription.field;
import static org.gama.safemodel.description.MethodDescription.method;

/**
 * @author Guillaume Mary
 */
public class MetaPhone<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaString<MetaPhone, FieldDescription<String>> number = new MetaString<>(field(Phone.class, "number", String.class));
	
	public MetaPhone() {
	}
	
	public MetaPhone(M accessor) {
		super(accessor);
		fixFieldsOwner();
	}
	
	public MetaString<MetaPhone, MethodDescription<String>> getNumber() {
		return new MetaString<MetaPhone, MethodDescription<String>>(method(Phone.class, "getNumber", String.class), this);
	}
	
}
