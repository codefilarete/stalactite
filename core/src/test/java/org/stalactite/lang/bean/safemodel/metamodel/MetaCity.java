package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.MetaModel;

/**
 * @author Guillaume Mary
 */
public class MetaCity<O extends MetaModel> extends MetaModel<O> {
	
	public MetaCity() {
	}
	
	public MetaCity(FieldDescription accessor) {
		super(accessor);
	}
}
