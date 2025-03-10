package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ExpandableSQL;
import org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter;
import org.codefilarete.stalactite.sql.statement.ExpandableStatement;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.ParsedSQL;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.trace.MutableInt;

/**
 * {@link SQLAppender} that can handle not-yet-set values from {@link ConditionalOperator}s as well as
 * already-set ones.
 * - Set values are those made of {@link ValuedVariable} and a numeric (incremental) placeholder is affected to them
 * - Not-yet-set values are placeholder ones made of {@link Placeholder} and the variable name is affected to them
 * Final result can be converted to a {@link PreparedSQL} with given values, see {@link #toPreparedSQL(Map)} 
 * 
 * @see #toPreparedSQL(Map)
 * @author Guillaume Mary
 */
public class ExpandableSQLAppender implements SQLAppender {
	
	/**
	 * Used to store SQL snippets
	 */
	private final List<Object> sqlSnippets = new ArrayList<>();
	private final DMLNameProvider dmlNameProvider;
	
	/**
	 * Current "String" in which expressions are appended. The instance is put into the {@link ParsedSQL} instance as a sql snippet.
	 * The reference changes as soon as a placeholder or a variable is added to {@link ExpandableSQLAppender}.
	 */
	private StringSQLAppender currentSQLSnippet;
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
	private final MutableInt paramCounter;
	
	public ExpandableSQLAppender(ColumnBinderRegistry parameterBinderRegistry, DMLNameProvider dmlNameProvider) {
		this.dmlNameProvider = dmlNameProvider;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.parameterBinders = new HashMap<>();
		this.values = new HashMap<>();
		this.paramCounter = new MutableInt();
		initCurrentSqlSnippet();
	}
	
	/**
	 * Private constructor for {@link #newSubPart(DMLNameProvider)}
	 * @param dmlNameProvider
	 * @param parameterBinderRegistry
	 * @param parameterBinders
	 * @param values
	 * @param paramCounter
	 */
	private ExpandableSQLAppender(
			DMLNameProvider dmlNameProvider,
			ColumnBinderRegistry parameterBinderRegistry,
			Map<String, ParameterBinder> parameterBinders,
			Map<String, Object> values,
			MutableInt paramCounter) {
		this.dmlNameProvider = dmlNameProvider;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.parameterBinders = parameterBinders;
		this.values = values;
		this.paramCounter = paramCounter;
		initCurrentSqlSnippet();
	}
	
	public List<Object> getSqlSnippets() {
		return sqlSnippets;
	}
	
	public Map<String, Object> getValues() {
		return values;
	}
	
	@Override
	public SQLAppender cat(String s, String... ss) {
		currentSQLSnippet.cat(s, ss);
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
				currentSQLSnippet.catColumn((Selectable) value);
			} else {
				addPlaceholder(value, getParameterBinderFromRegistry(value));
			}
		} else if (variable instanceof Placeholder) {
			addPlaceholder((Placeholder<?, ?>) variable);
		} else {
			addPlaceholder(variable, getParameterBinderFromRegistry(variable));
		}
		return this;
	}
	
	private ParameterBinder<?> getParameterBinderFromRegistry(Object value) {
		ParameterBinder<?> parameterBinder;
		if (value instanceof ValuedVariable) {
			parameterBinder = getParameterBinderFromRegistry(((ValuedVariable) value).getValue());
		} else if (value instanceof Placeholder) {
			parameterBinder = parameterBinderRegistry.getBinder(((Placeholder) value).getValueType());
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
				currentSQLSnippet.catColumn((Selectable) innerValue);
			} else {
				addPlaceholder(innerValue, binderSupplier);
			}
		} else if (value instanceof Placeholder) {
			addPlaceholder((Placeholder<?, ?>) value);
		} else {
			addPlaceholder(value, binderSupplier);
		}
		return this;
	}
	
	private void addPlaceholder(Placeholder<?, ?> variable) {
		sqlSnippets.add(variable);
		parameterBinders.put(variable.getName(), parameterBinderRegistry.getBinder(variable.getValueType()));
		initCurrentSqlSnippet();
	}
	
	private void addPlaceholder(Object value, ParameterBinder<?> binderSupplier) {
		String paramName = String.valueOf(paramCounter.increment());
		sqlSnippets.add(new Placeholder<>(paramName, binderSupplier.getColumnType()));
		parameterBinders.put(paramName, binderSupplier);
		values.put(paramName, value);
		initCurrentSqlSnippet();
	}
	
	protected void initCurrentSqlSnippet() {
		currentSQLSnippet = new StringSQLAppender(dmlNameProvider);
		sqlSnippets.add(currentSQLSnippet);
	}
	
	@Override
	public SQLAppender catColumn(Selectable<?> column) {
		// Columns are simply appended (no binder needed nor index increment)
		currentSQLSnippet.catColumn(column);
		return this;
	}
	
	@Override
	public SQLAppender catTable(Fromable table) {
		currentSQLSnippet.catTable(table);
		return this;
	}
	
	@Override
	public SQLAppender removeLastChars(int length) {
		currentSQLSnippet.removeLastChars(length);
		return this;
	}
	
	/**
	 * Implementation based on {@link ParsedSQL#toString()}. Not really useful for a real SQL statement, prefer {@link #toPreparedSQL(Map)} 
	 * 
	 * @return a representation of internal SQL snippets
	 */
	@Override
	public String getSQL() {
		return this.sqlSnippets.stream().map(sqlSnippet -> {
			if (sqlSnippet instanceof Placeholder) {
				return ":" + ((Placeholder<?, ?>) sqlSnippet).getName();
			} else if (sqlSnippet instanceof StringSQLAppender) {
				// inner SQL case
				return ((StringSQLAppender) sqlSnippet).getSQL();
			} else {
				throw new IllegalStateException("Unsupported SQL snippet: "
						+ (sqlSnippet == null ? "null" : Reflections.toString(sqlSnippet.getClass())));
			}
		}).collect(Collectors.joining());
	}
	
	@Override
	public SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider) {
		SubSQLAppender result = new DefaultSubSQLAppender(
				new ExpandableSQLAppender(
						dmlNameProvider,
						// we give all our attributes to make the subpart fill the global identifier counter and values, this avoids a complex
						// computation at close() time to propagate the value to the parent
						this.parameterBinderRegistry,
						this.parameterBinders,
						this.values,
						this.paramCounter)) {
			@Override
			public SQLAppender close() {
				// we ask for a new SQL snippet, it will reinitialize this.currentSQLSnippet and therefore prepare for another iteration as specified by newSubPart(..) 
				ExpandableSQLAppender.this.initCurrentSqlSnippet();
				return ExpandableSQLAppender.this;
			}
		};
		this.sqlSnippets.add(result);
		return result;
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
		ParsedSQL parsedSQL = new ParsedSQL();
		
		class ParsedSQLHelper {
			
			void add(ExpandableSQLAppender appender) {
				appender.sqlSnippets.forEach(sqlSnippet -> {
					if (sqlSnippet instanceof Placeholder) {
						add((Placeholder) sqlSnippet);
					} else if (sqlSnippet instanceof StringSQLAppender) {
						add((StringSQLAppender) sqlSnippet);
					} else if (sqlSnippet instanceof DefaultSubSQLAppender) {
						add((ExpandableSQLAppender) ((DefaultSubSQLAppender) sqlSnippet).getDelegate());
					}
				});
			}
			
			void add(Placeholder variable) {
				parsedSQL.addParam(variable.getName());
			}
			
			void add(StringSQLAppender sqlSnippet) {
				parsedSQL.addSqlSnippet(sqlSnippet.getSQL());
			}
		}
		
		new ParsedSQLHelper().add(this);
		
		ExpandableSQL expandableSQL = new ExpandableSQL(parsedSQL, valuesSizes);
		String placeholderSql = expandableSQL.getPreparedSQL();
		
		// Computing parameter binders for each "?" index
		Map<Integer, PreparedStatementWriter> placeholderBinders = new HashMap<>();
		Map<Integer, Object> placeholderValues = new HashMap<>();
		mergedValues.forEach((paramName, value) -> {
			ExpandableParameter expandableParameter = expandableSQL.getExpandableParameters().get(paramName);
			if (expandableParameter == null) {
				throw new BindingException("No parameter found in SQL for value named '" + paramName + "' : " + placeholderSql);
			}
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
