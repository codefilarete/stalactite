package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.List;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * A {@link OneToManyRelation} dedicated to {@link List} to configure indexation.
 * 
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of O
 */
public class OneToManyListRelation<SRC, TRGT, TRGTID, C extends List<TRGT>> extends OneToManyRelation<SRC, TRGT, TRGTID, C> {
	
	private Column indexingColumn;
	
	public <T extends Table> OneToManyListRelation(ReversibleAccessor<SRC, C> collectionProvider,
												   ValueAccessPointByMethodReference<SRC> methodReference,
												   EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration,
												   T targetTable) {
		super(collectionProvider, methodReference, targetMappingConfiguration, targetTable);
	}
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		this.indexingColumn = indexingColumn;
	}
	
	public <T extends Table, O extends Object> Column<T, O> getIndexingColumn() {
		return indexingColumn;
	}
}
