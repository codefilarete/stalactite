package org.codefilarete.stalactite.dsl.idpolicy;

import org.codefilarete.tool.Strings;

public interface DatabaseSequenceNamingStrategy {
	
	String giveName(Class<?> entityType);
	
	DatabaseSequenceNamingStrategy HIBERNATE_DEFAULT = entityType -> Strings.capitalize(entityType.getSimpleName()) + "_seq";
	
	DatabaseSequenceNamingStrategy DEFAULT = HIBERNATE_DEFAULT;
}
