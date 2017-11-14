package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentWhere;

/**
 * The interface defining what's possible to do (fluent point of view) after a from
 *
 * @author Guillaume Mary
 */
public interface FromTrailer extends WhereTrailer {
	
	FluentWhere where(Column column, CharSequence condition);
	
	FluentWhere where(Criteria criteria);
	
}
