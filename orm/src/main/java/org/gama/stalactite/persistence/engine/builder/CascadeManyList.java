package org.gama.stalactite.persistence.engine.builder;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A {@link CascadeMany} dedicated to {@link List} to configure indexation.
 * 
 * 
 * @param <SRC> the "one" type
 * @param <O> the "many" type
 * @param <J> identifier type of O
 */
public class CascadeManyList<SRC, O, J, C extends List<O>> extends CascadeMany<SRC, O, J, C> {
	
	private Column indexingColumn;
	
	public CascadeManyList(Function<SRC, C> targetProvider, Persister<O, J, ? extends Table> persister, Method method) {
		super(targetProvider, persister, (Class<C>) (Class) List.class, method);
	}
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		this.indexingColumn = indexingColumn;
	}
	
	public Column getIndexingColumn() {
		return indexingColumn;
	}
}
