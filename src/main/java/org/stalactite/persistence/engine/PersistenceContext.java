package org.stalactite.persistence.engine;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.sql.Dialect;

/**
 * @author mary
 */
public class PersistenceContext {
	
	private Dialect dialect;
	private DataSource dataSource;
	private Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
	public PersistenceContext(DataSource dataSource) {
		this(dataSource, determineDialect(dataSource));
	}
	
	public PersistenceContext(DataSource dataSource, Dialect dialect) {
		this.dataSource = dataSource;
		this.dialect = dialect;
	}
	
	public static Dialect determineDialect(DataSource dataSource) {
		return null;
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public <T> ClassMappingStrategy<T> getMappingStrategy(Class<T> aClass) {
		return mappingStrategies.get(aClass);
	}
	
	public void add(ClassMappingStrategy classMappingStrategy) {
		mappingStrategies.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
	}
}
