package org.gama.safemodel.lang;

import org.gama.safemodel.MetaMember;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaModel<MetaString, MethodDescription<Integer>> length = new MetaModel<>(MetaMember.method(String.class, "length", Integer.TYPE));
	
	public MetaString(M description) {
		super(description);
		length.setOwner(this);
	}
	
	public MetaModel<MetaString, MethodDescription<Integer>> length() {
		return length;
	}
}
