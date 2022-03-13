package org.codefilarete.stalactite.persistence.engine.configurer;

import java.util.List;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.persistence.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * A {@link CascadeMany} dedicated to {@link List} to configure indexation.
 * 
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of O
 */
public class CascadeManyList<SRC, TRGT, TRGTID, C extends List<TRGT>> extends CascadeMany<SRC, TRGT, TRGTID, C> {
	
	private Column indexingColumn;
	
	public <T extends Table> CascadeManyList(ReversibleAccessor<SRC, C> collectionProvider, ValueAccessPointByMethodReference methodReference,
											 EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration, T targetTable) {
		super(collectionProvider, methodReference, targetMappingConfiguration, targetTable);
	}
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		this.indexingColumn = indexingColumn;
	}
	
	public <T extends Table, O extends Object> Column<T, O> getIndexingColumn() {
		return indexingColumn;
	}
}
