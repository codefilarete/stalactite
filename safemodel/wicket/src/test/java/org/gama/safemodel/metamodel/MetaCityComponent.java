package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.CityComponent;

import static org.gama.safemodel.ComponentDescription.component;

/**
 * @author Guillaume Mary
 */
public class MetaCityComponent<M extends MetaModel, C extends Component> extends MetaModel<M, ComponentDescription<C>> {
	
	public MetaModel<MetaPhoneComponent, ComponentDescription<CityComponent>> name = new MetaModel<>(component(CityComponent.class, "name"));
	
	public MetaCityComponent() {
		fixFieldsOwner();
	}
	
	public MetaCityComponent(ComponentDescription<C> cityDescription) {
		super(cityDescription);
		fixFieldsOwner();
	}
}
