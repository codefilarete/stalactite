package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableDelete;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableUpdate;
import org.gama.stalactite.persistence.sql.Dialect;
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
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new Dialect());
		Table toto = new Table("toto");
		Column<Long> id = toto.addColumn("id", Long.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlSchemaGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		// test insert
		testInstance.insert(toto).set(id, 1L).execute();
		
		int updatedRowCount = testInstance.update(toto).set(id, 1L).execute();
		assertEquals(1, updatedRowCount);
		
		ResultSet select_id_from_toto = connection.createStatement().executeQuery("select id from toto");
		select_id_from_toto.next();
		assertEquals(1, select_id_from_toto.getInt(1));
		
		// test update with where
		ExecutableUpdate set = testInstance.update(toto).set(id, 2L);
		set.where(id, eq(1));
		updatedRowCount = set.execute();
		assertEquals(1, updatedRowCount);
		
		select_id_from_toto = connection.createStatement().executeQuery("select id from toto");
		select_id_from_toto.next();
		assertEquals(2, select_id_from_toto.getInt(1));
		
		// test delete
		ExecutableDelete delete = testInstance.delete(toto);
		delete.where(id, eq(2));
		assertEquals(1, delete.execute());
	}
	
}