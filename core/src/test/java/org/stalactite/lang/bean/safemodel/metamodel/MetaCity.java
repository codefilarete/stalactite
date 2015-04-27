package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.MetaModel;
import org.stalactite.lang.bean.safemodel.model.City;

/**
 * @author Guillaume Mary
 */
public class MetaCity<O extends MetaModel> extends MetaModel<O> {
	
	public MetaString name = new MetaString(field(City.class, "name"));
	
	public MetaCity() {
	}
	
	public MetaCity(FieldDescription accessor) {
		super(accessor);
		fixFieldsOwner();
	}
}
