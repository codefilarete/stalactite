package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.api.Selectable;

/**
 * @author guillaume.mary
 */
public interface Aliased {

	Selectable<?> as(String alias);

	String getAlias();
}
