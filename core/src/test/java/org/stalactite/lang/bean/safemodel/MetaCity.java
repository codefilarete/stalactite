package org.stalactite.lang.bean.safemodel;

import org.stalactite.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaCity<O extends MetaModel> extends MetaModel<O> {
	
	public MetaCity() {
	}
	
	public MetaCity(PropertyAccessor accessor) {
		super(accessor);
	}
}
