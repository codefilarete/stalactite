package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;

/**
 * Générateur d'id qui récupère les identifiants sur le résultat de l'ordre JDBC insert via {@link PreparedStatement#getGeneratedKeys()}
 * 
 * @author mary
 */
public interface PostInsertIdentifierGenerator extends IdentifierGenerator {
	
	// TODO: faire le contrat d'implémentation
}
