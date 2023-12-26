package org.codefilarete.stalactite.engine.runtime.load;

import java.util.function.Function;

import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ToBeanRowTransformer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Small interface which instances will be invoked after row transformation, such as one can add any post-treatment to the bean row
 * @param <C> the row bean
 */
@FunctionalInterface
public interface EntityTreeJoinNodeConsumptionListener<C> {
	
	/**
	 * Method invoked for each read row after all transformations made by a {@link AbstractTransformer} on a bean, so bean given as input is
	 * considered "complete".
	 *
	 * @param nodeEntity current row bean, may be different from row to row depending on bean instantiation policy of bean factory given
	 *  to {@link ToBeanRowTransformer} at construction time 
	 * @param columnValueProvider a function that let one read a value from current row without exposing internal mechanism of row reading
	 *  (which is tied to aliases of the SQL query built to load entity graph)
	 */
	void onNodeConsumption(C nodeEntity, Function<Column<?, ?>, Object> columnValueProvider);
}
