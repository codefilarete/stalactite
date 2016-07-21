package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaComponent;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.AddressComponent;
import org.gama.safemodel.component.CityComponent;
import org.gama.safemodel.component.PhoneComponent;

/**
 * @author Guillaume Mary
 */
public class MetaAddressComponent<O extends MetaModel, C extends Component> extends MetaComponent<O, C, AddressComponent> {
	
	public MetaCityComponent<MetaAddressComponent, AddressComponent> city = new MetaCityComponent<>(component(AddressComponent.class, "city", 
			CityComponent.class));
	
	public MetaPhoneComponent<MetaAddressComponent, AddressComponent> phone = new MetaPhoneComponent<>(component(AddressComponent.class, "phone", 
			PhoneComponent.class));
	
	public MetaAddressComponent() {
		fixFieldsOwner();
	}
	
	public MetaAddressComponent(ComponentDescription<C, AddressComponent> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
