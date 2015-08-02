package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class MetaMap<O extends MetaModel> extends MetaModel<O> {
	
	private MetaModel<MetaModel> get = new MetaModel<>(method(Map.class, "get", Object.class));
	
	private MetaModel<MetaModel> put = new MetaModel<>(method(Map.class, "put", Object.class, Object.class));
	
	public MetaMap() {
		get.setOwner(this);
		put.setOwner(this);
	}
	
	public MetaMap(AbstractMemberDescription description) {
		super(description);
	}
	
	public MetaMap(AbstractMemberDescription description, O owner) {
		super(description, owner);
	}
	
	public MetaModel put() {
		return put;
	}
}
