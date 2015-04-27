package org.stalactite.lang.bean.safemodel;

import java.util.List;

import org.stalactite.lang.bean.safemodel.MetaModel.GetterSetterDescription;

/**
 * @author Guillaume Mary
 */
public class ListElementAccessDescription extends GetterSetterDescription {
	
	public ListElementAccessDescription() {
		super(List.class, "get", "set", Integer.TYPE );
	}
}