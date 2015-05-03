package org.gama.safemodel.metamodel;

import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.model.City;

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
