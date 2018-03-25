package org.gama.stalactite.persistence.mapping;

import java.util.Map;
import java.util.Map.Entry;

import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;

/**
 * Class for transforming columns into multiple beans.
 * Some columns are given as markers for identifying entity. For each of these columns, a {@link IRowTransformer} must
 * be given (see constructor) which will be called to give an object (from the row partially or totally)).
 * Before {@link IRowTransformer} is called, ask to a "cached instance" is done to see if an already existing one
 * should be reused. Usefull to construct an object graph (see {@link #assembly(Object[])}. Cache is not part of this
 * class and must be implemented outside of it ({@link RowIterator for instance}.
 * If the identifier (the value of a marked column) is null then {@link IRowTransformer} is not called.
 * Finally, {@link #assembly(Object[])} is called to finalize construction of the transformed objects.
 *
 * @author Guillaume Mary
 */
public abstract class ToMultipleBeansRowTransformer<T> implements IRowTransformer<T> {
	
	private final Map<String, IRowTransformer> beanTransformers;
	
	/**
	 * Constructor with
	 *
	 * @param beanTransformers
	 */
	public ToMultipleBeansRowTransformer(Map<String, IRowTransformer> beanTransformers) {
		super();
		this.beanTransformers = beanTransformers;
	}
	
	@Override
	public T transform(Row row) {
		// Array of built instances on this row
		Object[] tranformedRow = new Object[this.beanTransformers.size()];
		int i = 0;
		for (Entry<String, IRowTransformer> entry : this.beanTransformers.entrySet()) {
			// ask for already existing instance for this column
			String colName = entry.getKey();
			Object colValue = row.get(colName);
			Object o = getCachedInstance(colName, colValue);
			if (o == null) {
				// no instance exists, so before asking for a new one, check if it's possible to give information
				// to build it: the grouping column as a value ?
				if (colValue != null) {
					o = entry.getValue().transform(row);
					onNewObject(colName, o);
				}
			}
			tranformedRow[i++] = o;
		}
		// ask for complementary actions
		assembly(tranformedRow);
		// we return the first object built, should be the same type as class generic types
		return (T) tranformedRow[0];
	}
	
	protected abstract Object getCachedInstance(String key, Object value);
	
	protected abstract void onNewObject(String key, Object o);
	
	protected abstract void assembly(Object[] rowAsObjects);
	
}
