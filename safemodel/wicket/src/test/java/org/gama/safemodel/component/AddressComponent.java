package org.gama.safemodel.component;

import org.apache.wicket.markup.html.panel.Panel;

/**
 * @author Guillaume Mary
 */
public class AddressComponent extends Panel {
	
	public AddressComponent(String id) {
		super(id);
	}
	
	@Override
	protected void onInitialize() {
		super.onInitialize();
		add(new CityComponent("city"));
		add(new PhoneComponent("phone"));
	}
}
