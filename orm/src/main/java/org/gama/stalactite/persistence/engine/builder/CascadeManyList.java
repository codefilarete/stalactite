package org.gama.stalactite.persistence.engine.builder;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
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
public class CascadeManyList<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> extends CascadeMany<SRC, O, J, List<O>> {
	
	private Column indexingColumn;
	
	public CascadeManyList(Function<SRC, ? extends List<O>> targetProvider, Persister<O, J, ? extends Table> persister, Method method) {
		super((Function<SRC, List<O>>) targetProvider, persister, (Class<List<O>>) (Class) List.class, method);
	}
	
	public void setIndexingColumn(Column indexingColumn) {
		this.indexingColumn = indexingColumn;
	}
	
	public Column getIndexingColumn() {
		return indexingColumn;
	}
}
