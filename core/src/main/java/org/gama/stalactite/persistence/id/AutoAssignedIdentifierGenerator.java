package org.gama.stalactite.persistence.id;

import java.io.Serializable;
import java.util.Map;

/**
 * Classe marqueuse pour les générateurs d'identifiant ... qu'il ne faut pas appeler car l'entité a positionné elle-même
 * son identifiant avant l'insert.
 * 
 * @author mary
 */
public class AutoAssignedIdentifierGenerator implements IdentifierGenerator {
	@Override
	public Serializable generate() {
		return null;
	}
	
	@Override
	public void configure(Map<String, Object> configuration) {
		
	}
}
