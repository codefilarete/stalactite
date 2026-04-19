package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * All subclasses share the parent table. A discriminator column distinguishes rows by their runtime type.
 *
 * @param <C> parent entity type
 * @param <I> identifier type
 * @param <D> discriminator value type (e.g. {@link String}, {@link Integer})
 * @param <T> the shared table type
 */
public class SingleTablePolymorphism<C, I, D, T extends Table<T>> implements EntityPolymorphism<C, I> {

    /** The shared table (same as rootEntity.getTable()) */
    private final Column<T, D> discriminatorColumn;
	
	/** Sub-entities keyed by their discriminator value. Insertion order is preserved for test stability. */
	private final Map<D, Mapping<? extends C, T>> subEntitiesPerDiscriminator = new KeepOrderMap<>();
	
	public SingleTablePolymorphism(Column<T, D> discriminatorColumn) {
		this.discriminatorColumn = discriminatorColumn;
	}
	
	public Column<T, D> getDiscriminatorColumn() {
		return discriminatorColumn;
	}
	
	public void addSubEntity(D discriminatorValue, Mapping<? extends C, T> subEntity) {
		subEntitiesPerDiscriminator.put(discriminatorValue, subEntity);
	}
	
	public Map<D, Mapping<? extends C, T>> getSubEntitiesPerDiscriminator() {
		return subEntitiesPerDiscriminator;
	}
	
	/** Returns the discriminator value associated with the given subclass type. */
	public D getDiscriminatorValue(Class<? extends C> subType) {
		return subEntitiesPerDiscriminator.entrySet().stream()
				.filter(e -> e.getValue().getEntityType().equals(subType))
				.map(Map.Entry::getKey)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No discriminator value found for type: " + subType));
	}
	
	@Override
	public Set<Mapping<? extends C, ?>> getSubEntities() {
		return new KeepOrderSet<>(subEntitiesPerDiscriminator.values());
	}
}
