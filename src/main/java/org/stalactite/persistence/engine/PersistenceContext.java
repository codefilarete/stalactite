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
	private Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	private DataSource dataSource;
	
	public PersistenceContext(DataSource dataSource) {
		this.dataSource = dataSource;
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
