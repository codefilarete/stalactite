package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaComponent;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.AddressComponent;
import org.gama.safemodel.component.PersonComponent;

/**
 * @author Guillaume Mary
 */
public class MetaPersonComponent<O extends MetaModel, C extends Component> extends MetaComponent<O, C, PersonComponent> {
	
	public MetaAddressComponent<MetaPersonComponent, PersonComponent> address = new MetaAddressComponent<>(component(PersonComponent.class, "address", AddressComponent.class));
	
	public MetaPersonComponent() {
		fixFieldsOwner();
	}
	
	public MetaPersonComponent(ComponentDescription<C, PersonComponent> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
