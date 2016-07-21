package org.gama.safemodel.component;

import org.apache.wicket.markup.html.panel.Panel;

/**
 * @author Guillaume Mary
 */
public class PersonComponent extends Panel {
	
	public PersonComponent(String id) {
		super(id);
	}
	
	@Override
	protected void onInitialize() {
		super.onInitialize();
		add(new AddressComponent("address"));
	}
}
