package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableDelete;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableUpdate;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;

import static org.gama.stalactite.query.model.Operand.eq;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextTest {
	
	@Test
	public void testInsert_Update_Delete() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new HSQLDBDialect());
		Table toto = new Table("toto");
		Column<Long> id = toto.addColumn("id", long.class);
		Column<String> name = toto.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlSchemaGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		// test insert
		testInstance.insert(toto).set(id, 1L).set(name, "Hello world !").execute();
		
		// test update
		int updatedRowCount = testInstance.update(toto).set(id, 1L).execute();
		assertEquals(1, updatedRowCount);
		
		ResultSet select_from_toto = connection.createStatement().executeQuery("select id, name from toto");
		select_from_toto.next();
		assertEquals(1, select_from_toto.getInt(1));
		assertEquals("Hello world !", select_from_toto.getString(2));
		
		// test update with where
		ExecutableUpdate set = testInstance.update(toto).set(id, 2L);
		set.where(id, eq(1L));
		updatedRowCount = set.execute();
		assertEquals(1, updatedRowCount);
		
		select_from_toto = connection.createStatement().executeQuery("select id from toto");
		select_from_toto.next();
		assertEquals(2, select_from_toto.getInt(1));
		
		// test delete
		ExecutableDelete delete = testInstance.delete(toto);
		delete.where(id, eq(2L));
		assertEquals(1, delete.execute());
	}
	
}