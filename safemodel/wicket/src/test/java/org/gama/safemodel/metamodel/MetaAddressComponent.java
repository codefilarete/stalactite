package org.gama.safemodel.metamodel;

import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.AddressComponent;
import org.gama.safemodel.component.CityComponent;
import org.gama.safemodel.component.PhoneComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaAddressComponent<M extends MetaModel> extends MetaModel<M, ComponentDescription<AddressComponent>> {
	
	public MetaCityComponent<MetaAddressComponent> city = new MetaCityComponent<>(component(CityComponent.class, "city"));
	
	public MetaPhoneComponent<MetaAddressComponent> phone = new MetaPhoneComponent<>(component(PhoneComponent.class, "phone"));
	
	public MetaAddressComponent() {
		fixFieldsOwner();
	}
	
	public MetaAddressComponent(ComponentDescription<AddressComponent> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
