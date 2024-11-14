package org.codefilarete.stalactite.query.model;

import javax.annotation.Nullable;

/**
 * Contract for elements to be put in a From clause, in order to be transformed to SQL
 * 
 * @author Guillaume Mary
 */
public interface Fromable extends SelectablesPod {
	
	@Nullable
	String getName();
	
	String getAbsoluteName();
}
