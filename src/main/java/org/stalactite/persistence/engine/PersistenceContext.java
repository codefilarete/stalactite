package org.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table;

/**
 * @author mary
 */
public class PersistenceContext {
	
	private Dialect dialect;
	private DataSource dataSource;
	private final Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
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
	
	public void deployDDL() throws SQLException {
		DDLGenerator ddlTableGenerator = getDDLGenerator();
		for (String sql : ddlTableGenerator.getCreationScripts()) {
			execute(sql);
		}
	}
	
	protected DDLGenerator getDDLGenerator() {
		List<Table> tablesToCreate = getTables();
		return new DDLGenerator(tablesToCreate, getDialect());
	}
	
	private List<Table> getTables() {
		List<Table> tablesToCreate = new ArrayList<>(this.mappingStrategies.size());
		for (ClassMappingStrategy classMappingStrategy : getMappingStrategies().values()) {
			tablesToCreate.add(classMappingStrategy.getTargetTable());
		}
		return tablesToCreate;
	}
	
	protected void execute(String sql) throws SQLException {
		try(Statement statement = getConnection().createStatement()) {
			statement.execute(sql);
		}
	}
	
	public <T> ClassMappingStrategy<T> getMappingStrategy(Class<T> aClass) {
		return mappingStrategies.get(aClass);
	}
	
	public void add(ClassMappingStrategy classMappingStrategy) {
		mappingStrategies.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
	}
	
	public Map<Class, ClassMappingStrategy> getMappingStrategies() {
		return mappingStrategies;
	}
	
	public Connection getConnection() throws SQLException {
		return getDataSource().getConnection();
	}
}
