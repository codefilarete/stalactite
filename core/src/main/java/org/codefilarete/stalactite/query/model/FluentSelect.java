package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public interface FluentSelect<SELF extends FluentSelect<SELF>>
		extends Iterable<Selectable<?> /* String, Column or AliasedColumn */>, SelectChain<SELF>, SelectablesPod {
	
}
