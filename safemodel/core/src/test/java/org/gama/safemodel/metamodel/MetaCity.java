package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.model.City;

/**
 * @author Guillaume Mary
 */
public class MetaCity<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaString<MetaCity, FieldDescription<String>> name = new MetaString<>(field(City.class, "name", String.class));
	
	public MetaCity() {
	}
	
	public MetaCity(M accessor) {
		super(accessor);
		fixFieldsOwner();
	}
}
