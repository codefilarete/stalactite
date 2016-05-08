package org.gama.safemodel.metamodel;

import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.AddressComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaPersonComponent<M extends MetaModel> extends MetaModel<M, ComponentDescription<AddressComponent>> {
	
	public MetaAddressComponent<MetaPersonComponent> address = new MetaAddressComponent<>(component(AddressComponent.class, "address"));
	
	public MetaPersonComponent() {
		fixFieldsOwner();
	}
	
	public MetaPersonComponent(ComponentDescription<AddressComponent> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
