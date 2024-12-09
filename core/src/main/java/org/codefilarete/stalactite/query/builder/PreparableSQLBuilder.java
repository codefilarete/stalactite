package org.codefilarete.stalactite.query.builder;

/**
 * @author Guillaume Mary
 */
public interface PreparableSQLBuilder {
	
	ExpandableSQLAppender toPreparableSQL();
}
