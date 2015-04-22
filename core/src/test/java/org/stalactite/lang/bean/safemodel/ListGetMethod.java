package org.stalactite.lang.bean.safemodel;

import java.util.List;

import org.stalactite.lang.bean.safemodel.MetaModel.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class ListGetMethod extends MethodDescription {
	
	public ListGetMethod(int index) {
		super(List.class, "get", new Object[] { index });
	}
}