package org.gama.safemodel.metamodel;

import org.apache.wicket.markup.html.basic.Label;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.PhoneComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaPhoneComponent<M extends MetaModel> extends MetaModel<M, ComponentDescription<PhoneComponent>> {
	
	public MetaModel<MetaPhoneComponent, ComponentDescription<Label>> number = new MetaModel<>(component(Label.class, "number"));
	
	public MetaPhoneComponent() {
		fixFieldsOwner();
	}
	
	public MetaPhoneComponent(ComponentDescription<PhoneComponent> phone) {
		super(phone);
		fixFieldsOwner();
	}
	
}
