package org.gama.safemodel.metamodel;

import org.apache.wicket.markup.html.basic.Label;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.CityComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaCityComponent<M extends MetaModel> extends MetaModel<M, ComponentDescription<CityComponent>> {
	
	public MetaModel<MetaPhoneComponent, ComponentDescription<Label>> name = new MetaModel<>(component(Label.class, "name"));
	
	public MetaCityComponent() {
		fixFieldsOwner();
	}
	
	public MetaCityComponent(ComponentDescription<CityComponent> cityDescription) {
		super(cityDescription);
		fixFieldsOwner();
	}
}
