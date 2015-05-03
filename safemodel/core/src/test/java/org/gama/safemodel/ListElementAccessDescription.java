package org.gama.safemodel;

import java.util.List;

import org.gama.safemodel.MetaModel.GetterSetterDescription;

/**
 * @author Guillaume Mary
 */
public class ListElementAccessDescription extends GetterSetterDescription {
	
	public ListElementAccessDescription() {
		super(List.class, "get", "set", Integer.TYPE );
	}
}