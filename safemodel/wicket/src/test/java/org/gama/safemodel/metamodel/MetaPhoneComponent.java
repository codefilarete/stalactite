package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.PhoneComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaPhoneComponent<M extends MetaModel, C extends Component> extends MetaModel<M, ComponentDescription<C>> {
	
	public MetaModel<MetaPhoneComponent, ComponentDescription<PhoneComponent>> number = new MetaModel<>(component(PhoneComponent.class, "number"));
	
	public MetaPhoneComponent() {
		fixFieldsOwner();
	}
	
	public MetaPhoneComponent(ComponentDescription<C> phone) {
		super(phone);
		fixFieldsOwner();
	}
	
}
