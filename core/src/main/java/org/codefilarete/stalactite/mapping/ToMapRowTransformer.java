package org.codefilarete.stalactite.mapping;

import java.util.Map;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Class for transforming columns into a Map
 * 
 * @author Guillaume Mary
 */
public abstract class ToMapRowTransformer<T extends Map> extends AbstractTransformer<T> {
	
	public ToMapRowTransformer(Class<T> clazz) {
		super(clazz);
	}
	
	/**
	 * Constructor with a general bean constructor
	 *
	 * @param factory the factory of beans
	 */
	public ToMapRowTransformer(Function<Function<Column<?, ?>, Object>, T> factory, ColumnedRow columnedRow) {
		super(factory, columnedRow);
	}
}
