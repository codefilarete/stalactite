package org.codefilarete.stalactite.mapping;

import java.util.Collection;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Class for transforming columns into a Collection.
 * 
 * @author Guillaume Mary
 */
public abstract class ToCollectionRowTransformer<T extends Collection> extends AbstractTransformer<T> {
	
	public ToCollectionRowTransformer(Class<T> clazz) {
		super(clazz);
	}
	
	/**
	 * Constructor with a general bean constructor
	 *
	 * @param factory the factory of beans
	 */
	public ToCollectionRowTransformer(Function<Function<Column<?, ?>, Object>, T> factory, ColumnedRow columnedRow) {
		super(factory, columnedRow);
	}
}
