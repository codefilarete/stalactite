package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.AddressComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaAddressComponent<M extends MetaModel, C extends Component> extends MetaModel<M, ComponentDescription<C>> {
	
	public MetaCityComponent<MetaAddressComponent, AddressComponent> city = new MetaCityComponent<>(component(AddressComponent.class, "city"));
	
	public MetaPhoneComponent<MetaAddressComponent, AddressComponent> phone = new MetaPhoneComponent<>(component(AddressComponent.class, "phone"));
	
	public MetaAddressComponent() {
		fixFieldsOwner();
	}
	
	public MetaAddressComponent(ComponentDescription<C> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
