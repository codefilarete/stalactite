package org.stalactite.persistence.id.sequence;

import static org.testng.Assert.assertEquals;

import java.io.Serializable;
import java.sql.SQLException;

import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.test.HSQLDBInMemoryDataSource;
import org.stalactite.test.JdbcTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PooledSequenceIdentifierGeneratorTest {
	
	private PooledSequenceIdentifierGenerator testInstance;
	
	@BeforeMethod
	public void setUp() throws SQLException {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		PersistenceContext.setCurrent(new PersistenceContext(new JdbcTransactionManager(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping)));
	}
	
	@AfterMethod
	public void tearDown() {
		PersistenceContext.clearCurrent();
	}
	
	@Test
	public void testGenerate() throws SQLException {
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Toto", PooledSequencePersistenceOptions.DEFAULT));
		PersistenceContext.getCurrent().deployDDL();
		// on vérifie que l'incrémentation se fait sans erreur depuis une base vierge
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals(newId, (long) i);
		}
		
		// on vérifie que l'incrémentation se fait sans erreur avec une nouvelle sequence sur la même table
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Tata", PooledSequencePersistenceOptions.DEFAULT));
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals(newId, (long) i);
		}
		
		// on vérifie que l'incrémentation se fait sans erreur avec sequence existante
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Toto", PooledSequencePersistenceOptions.DEFAULT));
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals(newId, (long) 50+i);
		}
	}
}