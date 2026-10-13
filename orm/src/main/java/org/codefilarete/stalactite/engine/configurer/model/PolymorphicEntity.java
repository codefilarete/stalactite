package org.codefilarete.stalactite.engine.configurer.model;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class PolymorphicEntity<C, I, T extends Table<T>> extends AbstractEntity<C, I, T> {
	
	private final EntityPolymorphism<C, I> polymorphism;
	
	public PolymorphicEntity(IdentifierMapping<C, I> identifierMapping, Mapping<C, T> mapping, EntityPolymorphism<C, I> polymorphism) {
		super(identifierMapping, mapping);
		this.polymorphism = polymorphism;
	}
	
	public EntityPolymorphism<C, I> getPolymorphism() {
		return polymorphism;
	}
	
	@Override
	public boolean isTablePerClass() {
		return this.polymorphism instanceof TablePerClassPolymorphism;
	}
	
	/**
	 * Adds a column to this table.
	 * May do nothing if a column already exists with same name and type.
	 * Will throw an exception if a column with same name but with different type already exists.
	 *
	 * @param name column name
	 * @param javaType column type
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	@Override
	public <O> Column<T, O> addColumn(String name, Class<O> javaType) {
		if (isTablePerClass()) {
			getPolymorphism().getSubEntities().forEach(subEntity -> subEntity.addColumn(name, javaType));
		}
		return getTable().addColumn(name, javaType);
	}
	
	/**
	 * Adds a column to this table.
	 * May do nothing if a column already exists with same name, size and type.
	 * Will throw an exception if a column with same name but with different type or size already exists.
	 *
	 * @param name column name
	 * @param javaType column type
	 * @param size column type size, null if type doesn't require any size ({@link #addColumn(String, Class)} should be preferred in this case)
	 *             but let as such for column copy use case
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	@Override
	public <O> Column<T, O> addColumn(String name, Class<O> javaType, @Nullable Size size) {
		if (isTablePerClass()) {
			getPolymorphism().getSubEntities().forEach(subEntity -> subEntity.addColumn(name, javaType, size));
		}
		return getTable().addColumn(name, javaType, size);
	}
	
	@Override
	public <O> Column<T, O> addColumn(String name, Class<O> javaType, @Nullable Size size, @Nullable Boolean nullable) {
		if (isTablePerClass()) {
			getPolymorphism().getSubEntities().forEach(subEntity -> subEntity.addColumn(name, javaType, size, nullable));
		}
		return getTable().addColumn(name, javaType, size, nullable);
	}
}
