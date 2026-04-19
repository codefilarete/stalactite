package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Each subclass has its own complete and independent table that copies all parent columns. No join is needed at the
 * SQL level: "select" query relies on a UNION across all subclass tables.
 *
 * @param <C> parent entity type
 * @param <I> identifier type
 */
public class TablePerClassPolymorphism<C, I> implements EntityPolymorphism<C, I> {
	
	/**
	 * Each sub-entity has its own standalone table that contains ALL columns (parent + child specific). No join is
	 * needed nor possible.
	 * The table type is per subclass, hence the wildcard.
	 */
	private final Map<Class<? extends C>, Mapping<? extends C, ?>> subEntities;
	
	public TablePerClassPolymorphism(Map<Class<? extends C>, Mapping<? extends C, ?>> subEntities) {
		this.subEntities = subEntities;
	}
	
	@Override
	public Set<Mapping<? extends C, ?>> getSubEntities() {
		return new KeepOrderSet<>(subEntities.values());
	}
}
