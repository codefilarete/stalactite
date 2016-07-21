package org.gama.safemodel.metamodel;

import org.apache.wicket.Component;
import org.apache.wicket.markup.html.basic.Label;
import org.gama.safemodel.ComponentDescription;
import org.gama.safemodel.MetaComponent;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.component.CityComponent;

/**
 * @author Guillaume Mary
 */
public class MetaCityComponent<O extends MetaModel, C extends Component> extends MetaComponent<O, C, CityComponent> {
	
	public MetaComponent<MetaCityComponent, CityComponent, Label> name = new MetaComponent<>(component(CityComponent.class, "name", Label.class));
	
	public MetaCityComponent() {
		fixFieldsOwner();
	}
	
	public MetaCityComponent(ComponentDescription<C, CityComponent> cityDescription) {
		super(cityDescription);
		fixFieldsOwner();
	}
}
