package org.codefilarete.stalactite.mapping;

import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Interface for classes capable of transforming a ResultSet row (represented by {@link Row} into any "more Object" instance.
 * 
 * @author Guillaume Mary
 */
public interface RowTransformer<C> {
	
	C transform(ColumnedRow row);
	
	C newBeanInstance(ColumnedRow row);
	
	void applyRowToBean(ColumnedRow row, C bean);
	
	void addTransformerListener(TransformerListener<? extends C> listener);
	
	/**
	 * Small interface which instances will be invoked after row transformation, such as one can add any post-treatment to the bean row
	 * @param <C> the row bean
	 */
	@FunctionalInterface
	interface TransformerListener<C> {
		
		/**
		 * Method invoked for each read row after all transformations made by a {@link AbstractTransformer} on a bean, so bean given as input is
		 * considered "complete".
		 *
		 * @param c current row bean, may be different from row to row depending on bean instantiation policy of bean factory given
		 * 		to {@link ToBeanRowTransformer} at construction time 
		 * @param rowValueProvider a function that let one read a value from current row without exposing internal mechanism of row reading.
		 *  Input is a {@link Column} because it is safer than a simple column name because {@link ToBeanRowTransformer} can be copied with
		 *  different aliases making mismatch when value is read from name.
		 */
		void onTransform(C c, Function<Column, Object> rowValueProvider);
		
	}
}
