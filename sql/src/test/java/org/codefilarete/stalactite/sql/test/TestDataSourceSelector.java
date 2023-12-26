package org.codefilarete.stalactite.sql.test;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class aimed at selecting an external service or an embedded one as DataSource.
 * Made to run tests in CI that provides some Database Service, or locally without no Database installed.
 * Choice is made according to presence of the JVM variable named {@value #DATABASE_SERVICES_VARIABLE_NAME} expected to point to a file path that contains databases coordinates
 * as properties. Each property name depends on database vendor and defined by subclasses (but is usually formed as "DatabaseVendorName.url").
 * 
 * Given file should contain several database service coordinates (one per database vendor), but may not contain all database vendor, depending
 * whether or not user wants to run tests on installed services or embedded ones.
 * 
 * @author Guillaume Mary
 */
public abstract class TestDataSourceSelector {
	
	private static final String DATABASE_SERVICES_VARIABLE_NAME = "databaseServices";
	
	private DataSource existingDatabaseService = null;
	
	public DataSource giveDataSource() {
		if (existingDatabaseService == null) {
			String databaseServicePath = System.getProperty(DATABASE_SERVICES_VARIABLE_NAME);
			if (databaseServicePath != null) {
				Map<String, String> databaseProperties = readProperties(databaseServicePath);
				if (isExternalServiceConfigured(databaseProperties)) {
					System.out.println("using datasource from file " + databaseServicePath);
					existingDatabaseService = buildDataSource(databaseProperties);
				} else {
					System.out.println("using embedded datasource");
					existingDatabaseService = buildEmbeddedDataSource();
				}
			} else {
				System.out.println("using embedded datasource");
				existingDatabaseService = buildEmbeddedDataSource();
			}
		}
		return existingDatabaseService;
	}
	
	private Map<String, String> readProperties(String databaseServicePath) {
		Properties databaseServiceConf = new Properties();
		try {
			databaseServiceConf.load(new FileReader(databaseServicePath));
		} catch (IOException e) {
			throw new RuntimeException("Can't read datasource properties from " + databaseServicePath, e);
		}
		return databaseServiceConf.stringPropertyNames().stream()
			.collect(Collectors.toMap(Function.identity(), databaseServiceConf::getProperty));
	}
	
	protected abstract boolean isExternalServiceConfigured(Map<String, String> properties);
	
	protected abstract DataSource buildDataSource(Map<String, String> properties);
	
	protected abstract DataSource buildEmbeddedDataSource();
}
