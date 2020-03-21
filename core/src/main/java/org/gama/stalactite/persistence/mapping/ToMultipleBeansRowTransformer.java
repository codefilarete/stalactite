package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.gama.stalactite.sql.result.Row;

/**
 * Class for transforming a {@link Row} into multiple beans.
 * Some columns are given as markers of bean identifier. For each of these columns, a {@link IRowTransformer} must
 * be given (see constructor), which will be asked to tranform given row as a bean. Hence multiple beans can be created from the row.
 * Finally, {@link #assemble(Map)} is called to finalize construction or assemble beans.
 * 
 * Before asking {@link IRowTransformer} for new bean, a check for a cached instance is done (through {@link #getCachedInstance(String, Object)})
 * so one can reuse a bean to complete it : this is made to construct an object graph from several rows.
 * Cache is not part of this class and must be implemented outside of it.
 * If the identifier (the value of a marked column) is null then {@link IRowTransformer} is not called.
 *
 * @author Guillaume Mary
 */
public abstract class ToMultipleBeansRowTransformer<T> implements IRowTransformer<T> {
	
	private final Map<String, IRowTransformer> beanTransformers;
	
	private final String keyToReturn;
	
	/**
	 * Constructor with
	 *
	 * @param beanTransformers bean mappers, key is the column name for bean identifiers 
	 * @param keyToReturn "main" column name : which contains bean that must be returned by {@link #transform(Row)}
	 */
	public ToMultipleBeansRowTransformer(Map<String, IRowTransformer> beanTransformers, String keyToReturn) {
		super();
		this.beanTransformers = beanTransformers;
		this.keyToReturn = keyToReturn;
	}
	
	@Override
	public T transform(Row row) {
		// Array of built instances on this row
		Map<String, Object> tranformedRow = new HashMap<>(beanTransformers.size());
		for (Entry<String, IRowTransformer> entry : this.beanTransformers.entrySet()) {
			// ask for already existing instance for this column
			String colName = entry.getKey();
			Object colValue = row.get(colName);
			if (colValue != null) {
				Object o = getCachedInstance(colName, colValue);
				if (o == null) {
					// no instance exists, so before asking for a new one, check if it's possible to give information
					// to build it: the grouping column as a value ?
					o = entry.getValue().transform(row);
					onNewObject(colName, o);
				}
				tranformedRow.put(colName, o);
			}
		}
		// ask for complementary actions
		assemble(tranformedRow);
		// we return the first object built, should be the same type as class generic types
		return (T) tranformedRow.get(keyToReturn);
	}
	
	protected abstract Object getCachedInstance(String key, Object value);
	
	protected abstract void onNewObject(String key, Object o);
	
	protected abstract void assemble(Map<String, Object> rowAsObjects);
	
}
