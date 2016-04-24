package org.gama.safemodel;

import org.gama.safemodel.description.AbstractMemberDescription;

/**
 * Parent for MetaModel that describes a member of a class
 * 
 * @author Guillaume Mary
 */
public class MetaMember<O extends MetaModel, D extends AbstractMemberDescription> extends MetaModel<O, D> {
	
	protected MetaMember() {
	}
	
	public MetaMember(D description) {
		super(description);
	}
	
	public MetaMember(D description, O owner) {
		
	}
}
