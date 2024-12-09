package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnvaluedVariable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ExpandableSQL;
import org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter;
import org.codefilarete.stalactite.sql.statement.ExpandableStatement;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.ParsedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * {@link SQLAppender} that can handle not-yet-set values from {@link org.codefilarete.stalactite.query.model.ConditionalOperator}s as well as
 * already-set ones.
 * - Set values are those made of {@link ValuedVariable} and a numeric (incremental) placeholder is affected to them
 * - Not-yet-set values are placeholder ones made of {@link UnvaluedVariable} and the variable name is affected to them
 * Final result can be converted to a {@link PreparedSQL} with given values, see {@link #toPreparedSQL(Map)} 
 * 
 * @see #toPreparedSQL(Map)
 * @author Guillaume Mary
 */
public class ExpandableSQLAppender implements SQLAppender {
	
	/**
	 * Used to store SQL snippets
	 */
	private final ParsedSQL parsedSQL;
	
	/**
	 * Current "String" in which expressions are appended. The instance is put into the {@link ParsedSQL} instance as a sql snippet.
	 * The reference changes as soon as a placeholder or a variable is added to {@link ExpandableSQLAppender}.
	 */
	private StringAppender currentSQLSnippet = new StringAppender();
	private final ColumnBinderRegistry parameterBinderRegistry;
	/**
	 * Collected {@link ParameterBinder} per variable name
	 */
	private final Map<String, ParameterBinder> parameterBinders;
	/**
	 * Collected values per variable name
	 */
	private final Map<String, Object> values;
	/**
	 * Counter for unnamed variables : each time a raw value is appended to the {@link ExpandableSQLAppender} instance, this counter is incremented
	 * and a variable name is created for it, simply made of the Integer as a String
	 */
	private final ModifiableInt paramCounter = new ModifiableInt();
	private final DMLNameProvider dmlNameProvider;
	
	public ExpandableSQLAppender(ColumnBinderRegistry parameterBinderRegistry, DMLNameProvider dmlNameProvider) {
		this.parsedSQL = new ParsedSQL();
		this.parsedSQL.addSqlSnippet(currentSQLSnippet);
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.dmlNameProvider = dmlNameProvider;
		this.parameterBinders = new HashMap<>();
		this.values = new HashMap<>();
	}
	
	public ParsedSQL getParsedSQL() {
		return parsedSQL;
	}
	
	public Map<String, Object> getValues() {
		return values;
	}
	
	public Map<String, ParameterBinder> getParameterBinders() {
		return parameterBinders;
	}
	
	@Override
	public SQLAppender cat(String s, String... ss) {
		currentSQLSnippet.cat(s).cat(ss);
		return this;
	}
	
	@Override
	public <V> SQLAppender catValue(@Nullable Selectable<V> column, Object value) {
		ParameterBinder<?> parameterBinder;
		if (column == null) {
			parameterBinder = getParameterBinderFromRegistry(value);
		} else if (column instanceof Column) {
			parameterBinder = parameterBinderRegistry.getBinder((Column) column);
		} else {
			parameterBinder = parameterBinderRegistry.getBinder(column.getJavaType());
		}
		return catValue(value, parameterBinder);
	}
	
	@Override
	public SQLAppender catValue(Object variable) {
		if (variable instanceof ValuedVariable) {
			Object value = ((ValuedVariable) variable).getValue();
			if (value instanceof Selectable) {
				// Columns are simply appended (no binder needed nor index increment)
				currentSQLSnippet.cat(dmlNameProvider.getName((Selectable) value));
			} else {
				handleValuePlaceholder(value, getParameterBinderFromRegistry(value));
			}
		} else if (variable instanceof UnvaluedVariable) {
			handleVariablePlaceholder((UnvaluedVariable<?, ?>) variable);
		} else {
			handleValuePlaceholder(variable, getParameterBinderFromRegistry(variable));
		}
		return this;
	}
	
	private ParameterBinder<?> getParameterBinderFromRegistry(Object value) {
		ParameterBinder<?> parameterBinder;
		if (value instanceof ValuedVariable) {
			parameterBinder = getParameterBinderFromRegistry(((ValuedVariable) value).getValue());
		} else if (value instanceof UnvaluedVariable) {
			parameterBinder = parameterBinderRegistry.getBinder(((UnvaluedVariable) value).getValueType());
		} else {
			Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
			parameterBinder = parameterBinderRegistry.getBinder(binderType);
		}
		return parameterBinder;
	}
	
	private SQLAppender catValue(Object value, ParameterBinder<?> binderSupplier) {
		if (value instanceof ValuedVariable) {
			Object innerValue = ((ValuedVariable) value).getValue();
			if (innerValue instanceof Selectable) {
				// Columns are simply appended (no binder needed nor index increment)
				currentSQLSnippet.cat(dmlNameProvider.getName((Selectable) innerValue));
			} else {
				handleValuePlaceholder(innerValue, binderSupplier);
			}
		} else if (value instanceof UnvaluedVariable) {
			handleVariablePlaceholder((UnvaluedVariable<?, ?>) value);
		} else {
			handleValuePlaceholder(value, binderSupplier);
		}
		return this;
	}
	
	private void handleVariablePlaceholder(UnvaluedVariable<?, ?> variable) {
		parsedSQL.addParam(variable.getName());
		parameterBinders.put(variable.getName(), parameterBinderRegistry.getBinder(variable.getValueType()));
		nextSqlSnippet();
	}
	
	private void handleValuePlaceholder(Object value, ParameterBinder<?> binderSupplier) {
		String paramName = String.valueOf(paramCounter.increment());
		parsedSQL.addParam(paramName);
		parameterBinders.put(paramName, binderSupplier);
		values.put(paramName, value);
		nextSqlSnippet();
	}
	
	private void nextSqlSnippet() {
		currentSQLSnippet = new StringAppender();
		parsedSQL.addSqlSnippet(currentSQLSnippet);
	}
	
	@Override
	public SQLAppender catColumn(Column column) {
		// Columns are simply appended (no binder needed nor index increment)
		currentSQLSnippet.cat(dmlNameProvider.getName(column));
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		currentSQLSnippet.cutTail(length);
		return this;
	}
	
	/**
	 * Implementation based on {@link ParsedSQL#toString()}. Not really useful for real a SQL statement, prefer {@link #toPreparedSQL(Map)} 
	 * 
	 * @return a representation of internal SQL snippets
	 */
	@Override
	public String getSQL() {
		return parsedSQL.toString();
	}
	
	/**
	 * Creates a {@link PreparedSQL} from given values.
	 * Given values are merged with those of current instance, hence only named placeholders values are required.
	 * However, values present in original SQL can be overwritten by giving a value to their numeric placeholder. 
	 * 
	 * @param values values per named placeholder
	 * @return a new {@link PreparedSQL} for given values set
	 */
	public PreparedSQL toPreparedSQL(Map<String, Object> values) {
		Map<String, Object> mergedValues = new HashMap<>(getValues());
		mergedValues.putAll(values);
		// we unwrap Variables from values because PreparedSQL doesn't support it  
		unwrapVariables(mergedValues);
		
		// we ask ExpandableSQL to build the SQL made of "?" for each of our values
		Map<String, Integer> valuesSizes = ExpandableSQL.sizes(mergedValues);
		ExpandableSQL expandableSQL = new ExpandableSQL(this.parsedSQL, valuesSizes);
		String placeholderSql = expandableSQL.getPreparedSQL();
		
		// Computing parameter binders for each "?" index
		Map<Integer, PreparedStatementWriter> placeholderBinders = new HashMap<>();
		Map<Integer, Object> placeholderValues = new HashMap<>();
		mergedValues.forEach((paramName, value) -> {
			ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(paramName);
			int[] markIndexes = expandableParameter.getMarkIndexes();
			for (int markIndex : markIndexes) {
				placeholderBinders.put(markIndex, parameterBinders.get(paramName));
			}
			ExpandableStatement.adaptIterablePlaceholders(value, markIndexes, placeholderValues::put);
		});
		
		PreparedSQL result = new PreparedSQL(placeholderSql, placeholderBinders);
		result.setValues(placeholderValues);
		return result;
	}
	
	/**
	 * Replaces {@link ValuedVariable} values by their concrete values.
	 * Note that this method replaces values of the given {@link Map} (for simplicity of calling algorithm)
	 * 
	 * @param mergedValues {@link Map} in which {@link ValuedVariable} must be replaced by their own internal values
	 */
	private void unwrapVariables(Map<String, Object> mergedValues) {
		mergedValues.entrySet().forEach(entry -> {
			if (entry.getValue() instanceof ValuedVariable) {
				entry.setValue(((ValuedVariable<?>) entry.getValue()).getValue());
			}
		});
	}
}
