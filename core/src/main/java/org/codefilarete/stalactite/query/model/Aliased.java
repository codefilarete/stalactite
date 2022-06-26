package org.codefilarete.stalactite.query.model;

/**
 * @author guillaume.mary
 */
public interface Aliased {

	Selectable<?> as(String alias);

	String getAlias();
}
