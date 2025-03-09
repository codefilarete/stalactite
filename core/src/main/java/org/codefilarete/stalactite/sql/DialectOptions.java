package org.codefilarete.stalactite.sql;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Hanger.Holder;

/**
 * {@link DialectOptions} are used to adapt the SQL dialect of the database.
 * @author Guillaume Mary
 */
public class DialectOptions {
	
	private final OptionalSetting<Integer> inOperatorMaxSize = new OptionalSetting<>();
	private final OptionalSetting<Character> quoteCharacter = new OptionalSetting<>();
	private final OptionalSetting<Boolean> quoteSQLIdentifiers = new OptionalSetting<>();
	private final OptionalSetting<Set<JavaTypeToSQLType<?>>> javaTypeToSqlTypeMappings = new OptionalSetting<>();
	private final OptionalSetting<Set<JavaTypeBinder<?>>> javaTypeBinders = new OptionalSetting<>();
	
	public OptionalSetting<Integer> getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public DialectOptions setInOperatorMaxSize(int inOperatorMaxSize) {
		this.inOperatorMaxSize.set(inOperatorMaxSize);
		return this;
	}
	
	public OptionalSetting<Character> getQuoteCharacter() {
		return quoteCharacter;
	}
	
	public DialectOptions setQuoteCharacter(char quoteCharacter) {
		this.quoteCharacter.set(quoteCharacter);
		return this;
	}
	
	public OptionalSetting<Boolean> getQuoteSQLIdentifiers() {
		return quoteSQLIdentifiers;
	}
	
	public DialectOptions setQuoteSQLIdentifiers(boolean quoteSQLIdentifiers) {
		this.quoteSQLIdentifiers.set(quoteSQLIdentifiers);
		return this;
	}
	
	public DialectOptions quoteSQLIdentifiers() {
		return setQuoteSQLIdentifiers(true);
	}
	
	public OptionalSetting<Set<JavaTypeBinder<?>>> getJavaTypeBinders() {
		return javaTypeBinders;
	}
	
	public OptionalSetting<Set<JavaTypeToSQLType<?>>> getJavaTypeToSqlTypeMappings() {
		return javaTypeToSqlTypeMappings;
	}
	
	/**
	 * Defines the persistence of a mono-column Java type.
	 *
	 * @param javaType the Java type to define
	 * @param sqlType the mapped SQL type (for database schema generation)
	 * @param parameterBinder Java type binding (for JDBC statements usage)
	 * @return this
	 */
	public <T> DialectOptions addTypeBinding(Class<T> javaType, String sqlType, ParameterBinder<T> parameterBinder) {
		setJavaTypeToSQL(javaType, sqlType);
		setTypeBinding(javaType, parameterBinder);
		return this;
	}
	
	/**
	 * Set or override a type definition (for database schema generation), either to define a new one or to change the SQL type of an existing one.
	 * In the former case, invoker shall also use {@link #setTypeBinding(Class, ParameterBinder)} to define its binding. Which means that
	 * {@link #addTypeBinding(Class, String, ParameterBinder)} should be preferred.
	 * 
	 * @param javaType the Java type to be set
	 * @param sqlType the mapped SQL type 
	 * @return this
	 */
	public DialectOptions setJavaTypeToSQL(Class<?> javaType, String sqlType) {
		if (this.javaTypeToSqlTypeMappings.get() == null) {
			this.javaTypeToSqlTypeMappings.set(new HashSet<>());
		}
		this.javaTypeToSqlTypeMappings.get().add(new JavaTypeToSQLType<>(javaType, sqlType));
		return this;
	}
	
	/**
	 * Set or override a type binding (for JDBC statements usage), either to define a new one or to change the binding of an existing one.
	 * In the former case, invoker shall also use {@link #setJavaTypeToSQL(Class, String)} to define its SQL type. Which means that
	 * {@link #addTypeBinding(Class, String, ParameterBinder)} should be preferred.
	 *
	 * @param javaType the Java type to be set
	 * @param parameterBinder the mapped SQL type 
	 * @return this
	 * @param <T> Java type
	 */
	public <T> DialectOptions setTypeBinding(Class<T> javaType, ParameterBinder<T> parameterBinder) {
		if (this.javaTypeBinders.get() == null) {
			this.javaTypeBinders.set(new HashSet<>());
		}
		this.javaTypeBinders.get().add(new JavaTypeBinder<>(javaType, parameterBinder));
		return this;
	}
	
	public static class JavaTypeToSQLType<T> {
		
		private final Class<T> javaType;
		private final String sqlType;
		
		private JavaTypeToSQLType(Class<T> javaType, String sqlType) {
			this.javaType = javaType;
			this.sqlType = sqlType;
		}
		
		public Class<T> getJavaType() {
			return javaType;
		}
		
		public String getSqlType() {
			return sqlType;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			JavaTypeToSQLType<?> that = (JavaTypeToSQLType<?>) o;
			return javaType.equals(that.javaType);
		}
		
		@Override
		public int hashCode() {
			return javaType.hashCode();
		}
	}
	

	public static class JavaTypeBinder<T> {

		private final Class<T> javaType;
		private final ParameterBinder<T> parameterBinder;

		private JavaTypeBinder(Class<T> javaType, ParameterBinder<T> parameterBinder) {
			this.javaType = javaType;
			this.parameterBinder = parameterBinder;
		}

		public Class<T> getJavaType() {
			return javaType;
		}

		public ParameterBinder<T> getParameterBinder() {
			return parameterBinder;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			JavaTypeBinder<?> that = (JavaTypeBinder<?>) o;
			return javaType.equals(that.javaType);
		}
		
		@Override
		public int hashCode() {
			return javaType.hashCode();
		}
	}

	public static class OptionalSetting<T> extends Holder<T> {
		
		private boolean touched;
		
		private OptionalSetting() {
		}
		
		@Override
		public void set(T value) {
			super.set(value);
			touched = true;
		}
		
		public boolean isTouched() {
			return touched;
		}
		
		public T getOrDefault(T defaultValue) {
			return isTouched() ? super.get() : defaultValue;
		}
		
		public void consumeIfTouched(Consumer<T> consumer) {
			if (isTouched()) {
				consumer.accept(get());
			}
		}
	}
}
