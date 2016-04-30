package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.model.Address;
import org.gama.safemodel.model.Person;

import static org.gama.safemodel.description.FieldDescription.*;

/**
 * @author Guillaume Mary
 */
public class MetaPerson<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaAddress<MetaPerson, FieldDescription<Address>> address = new MetaAddress<>(field(Person.class, "address", Address.class));
	
	public MetaPerson() {
		fixFieldsOwner();
	}
	
	public MetaPerson(M accessor) {
		super(accessor);
		fixFieldsOwner();
//		this.address.setOwner(this);
	}
}
