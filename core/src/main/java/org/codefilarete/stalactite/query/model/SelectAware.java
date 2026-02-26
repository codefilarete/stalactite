package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.api.SelectChain;
import org.codefilarete.stalactite.query.api.Selectable;

public interface SelectAware<SELF extends SelectAware<SELF>>  {
	
	SELF select(Iterable<? extends Selectable<?>> selectables);
	
	SELF select(Selectable<?> expression, Selectable<?>... expressions);
	
	SelectChain.Aliasable<SELF> select(String expression, Class<?> javaType);
	
	SELF select(String expression, Class<?> javaType, String alias);
	
	SelectChain.Aliasable<SELF> select(Selectable<?> column);
	
	SELF select(Selectable<?> column, String alias);
	
	SELF select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2);
	
	SELF select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3);
	
	SELF select(Map<? extends Selectable<?>, String> aliasedColumns);
}
