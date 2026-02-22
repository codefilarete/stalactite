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
	
	AliasableExpression<SELF> add(String expression, Class<?> javaType);
	
	default SELF add(Selectable<?> column) {
		return add(column, (String) null);
	}
	
	SELF add(Selectable<?> column, String alias);
	
	default SELF add(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		return add(col1, alias1).add(col2, alias2);
	}
	
	default SELF add(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		return add(col1, alias1).add(col2, alias2).add(col3, alias3);
	}
	
	SELF add(Map<? extends Selectable<?>, String> aliasedColumns);
	
	SELF distinct();
	
	default SELF getSelect() {
		return (SELF) this;
	}
	
	interface Aliasable {
		
		SelectChain<?> as(String alias);
	}
	
	/**
	 * A mixin to chain {@link Aliasable} and {@link SelectChain}.
	 * 
	 * @param <SELF> type of {@link SelectChain}
	 */
	interface AliasableExpression<SELF extends SelectChain<SELF>> extends Aliasable, SelectChain<SELF> {
		
	}
}
