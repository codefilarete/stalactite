package org.gama.stalactite.persistence.sql.dml.binder;

import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.lang.test.Assertions.assertThrows;
import static org.gama.lang.test.Assertions.hasExceptionInCauses;
import static org.gama.lang.test.Assertions.hasMessage;

/**
 * @author Guillaume Mary
 */
class ColumnBinderRegistryTest {
	
	@Test
	void register_columnAlreadyRegistered_throwsException() {
		ColumnBinderRegistry testInstance = new ColumnBinderRegistry();
		
		Table table = new Table("toto");
		Column nameColumn = table.addColumn("name", String.class);
		testInstance.register(nameColumn, DefaultParameterBinders.STRING_BINDER);
		
		// registering the same binder has no consequence
		testInstance.register(nameColumn, DefaultParameterBinders.STRING_BINDER);
		// but doing it with a different binder throws an exception
		assertThrows(() -> testInstance.register(nameColumn, DefaultParameterBinders.INTEGER_BINDER), hasExceptionInCauses(BindingException.class)
				.andProjection(hasMessage("Binder for column toto.name already exists")));
	}
	
}