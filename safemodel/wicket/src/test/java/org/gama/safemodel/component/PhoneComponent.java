package org.gama.safemodel.component;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.Panel;

/**
 * @author Guillaume Mary
 */
public class PhoneComponent extends Panel {
	public PhoneComponent(String id) {
		super(id);
	}
	
	@Override
	protected void onInitialize() {
		super.onInitialize();
		add(new Label("number"));
	}
}
