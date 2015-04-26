package org.stalactite.lang.bean.safemodel;

import java.util.List;

import org.stalactite.lang.bean.safemodel.MetaModel.AccessorDescription;

/**
 * @author Guillaume Mary
 */
public class ListElementAccessDefinition extends AccessorDescription {
	
	public ListElementAccessDefinition() {
		super(List.class, "get", "set", Integer.TYPE );
	}
}