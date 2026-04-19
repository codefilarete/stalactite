package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.mapping.id.manager.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class CompositeIdentifierMapping<C, I, T extends Table<T>> extends IdentifierMapping<C, I> {
	
	private final EmbeddableMapping<I, T> mappingConfiguration;
	
	public CompositeIdentifierMapping(ReadWritePropertyAccessPoint<C, I> idAccessor,
	                                  CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> identifierInsertionManager,
	                                  EmbeddableMapping<I, T> propertiesMapping) {
		super(idAccessor, identifierInsertionManager);
		this.mappingConfiguration = propertiesMapping;
	}
	
	@Override
	public CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> getIdentifierInsertionManager() {
		return (CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I>) super.getIdentifierInsertionManager();
	}
	
	public EmbeddableMapping<I, T> getCompositeKeyMapping() {
		return mappingConfiguration;
	}
}
