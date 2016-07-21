package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.apache.wicket.markup.html.basic.Label;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaComponent;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.PhoneComponent;

/**
 * @author Guillaume Mary
 */
public class MetaPhoneComponent<O extends MetaModel, C extends Component> extends MetaComponent<O, C, PhoneComponent> {
	
	public MetaComponent<MetaPhoneComponent, PhoneComponent, Label> number = new MetaComponent<>(component(PhoneComponent.class, "number", Label.class));
	
	public MetaPhoneComponent() {
		fixFieldsOwner();
	}
	
	public MetaPhoneComponent(ComponentDescription<C, PhoneComponent> phone) {
		super(phone);
		fixFieldsOwner();
	}
	
}
