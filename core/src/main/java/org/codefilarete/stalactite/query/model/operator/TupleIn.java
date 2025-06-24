package org.codefilarete.stalactite.query.model.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Represents a "in" that accepts tuples (not supported by all databases)
 *
 * @author Guillaume Mary
 */
public class TupleIn extends ConditionalOperator<Object[], List<Object[]>> {
	
	/**
	 * Constructs a {@link TupleIn} object from the given map of columns that represents the properties values of a bean.
	 * The given {@link Map} usually contains a {@link List} as values, the sizes of the lists are expected to be same and equal to inSize.
	 * 
	 * @param inSize number of elements of the "in" clause
	 * @param values bean values per properties as keys
	 * @return a new {@link TupleIn} filled with bean values
	 */
	public static TupleIn transformBeanColumnValuesToTupleInValues(int inSize, Map<? extends Column<?, ?>, ?> values) {
		List<Object[]> resultValues = new ArrayList<>(inSize);
		
		Column<?, ?>[] columns = new ArrayList<>(values.keySet()).toArray(new Column[0]);
		for (int i = 0; i < inSize; i++) {
			List<Object> beanValues = new ArrayList<>(columns.length);
			for (Column<?, ?> column: columns) {
				Object value = values.get(column);
				// we respect initial will as well as ExpandableStatement.doApplyValue(..) algorithm
				if (value instanceof List) {
					beanValues.add(((List) value).get(i));
				} else {
					beanValues.add(value);
				}
			}
			resultValues.add(beanValues.toArray());
		}
		
		return new TupleIn(columns, resultValues);
	}
	
	private final Column[] columns;
	
	private Variable<List<Object[]>> values;
	
	public TupleIn(Column[] columns, Variable<List<Object[]>> values) {
		this.columns = columns;
		this.values = values;
	}
	
	public TupleIn(Column[] columns, List<Object[]> values) {
		this(columns, new ValuedVariable<>(values));
	}
	
	public Column[] getColumns() {
		return columns;
	}
	
	public Variable<List<Object[]>> getValue() {
		return values;
	}
	
	@Override
	public void setValue(Variable<List<Object[]>> value) {
		this.values = value;
	}
	
	@Override
	public boolean isNull() {
		return false;
	}
}
