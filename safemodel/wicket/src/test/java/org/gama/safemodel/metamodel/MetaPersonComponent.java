package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.PersonComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaPersonComponent<M extends MetaModel, C extends Component> extends MetaModel<M, ComponentDescription<C>> {
	
	public MetaAddressComponent<MetaPersonComponent, PersonComponent> address = new MetaAddressComponent<>(component(PersonComponent.class, "address"));
	
	public MetaPersonComponent() {
		fixFieldsOwner();
	}
	
	public MetaPersonComponent(ComponentDescription<C> addressComponentDescription) {
		super(addressComponentDescription);
		fixFieldsOwner();
	}
}
