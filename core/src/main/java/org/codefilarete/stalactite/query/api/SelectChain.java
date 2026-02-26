package org.codefilarete.stalactite.query.api;

import java.util.Map;

/**
 * The interface defining what's possible to do (fluent point of view) on a select
 * 
 * @author Guillaume Mary
 */
public interface SelectChain<SELF extends SelectChain<SELF>> {
	
	SELF add(Iterable<? extends Selectable<?>> expressions);
	
	SELF add(Selectable<?> expression, Selectable<?>... expressions);
	
	SELF add(String expression, Class<?> javaType);
	
	SELF add(String expression, Class<?> javaType, String alias);
	
	SELF add(Selectable<?> column);
	
	SELF add(Selectable<?> column, String alias);
	
	default SELF add(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		return add(col1, alias1).add(col2, alias2);
	}
	
	default SELF add(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		return add(col1, alias1).add(col2, alias2).add(col3, alias3);
	}
	
	SELF add(Map<? extends Selectable<?>, String> aliasedColumns);
	
	SELF distinct();
	
	interface Aliasable<SELF> {
		
		SELF as(String alias);
	}
}
