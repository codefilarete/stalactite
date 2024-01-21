package org.codefilarete.stalactite.sql.statement.binder;

import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
		assertThatThrownBy(() -> testInstance.register(nameColumn, DefaultParameterBinders.INTEGER_BINDER))
				.extracting(t -> Exceptions.findExceptionInCauses(t, BindingException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Binder for column toto.name already exists");
	}
	
	@Test
	void doGetBinder_columnIsNotRegistered_throwsException() {
		ColumnBinderRegistry testInstance = new ColumnBinderRegistry();
		Table table = new Table("toto");
		Column setColumn = table.addColumn("set", Set.class);
		assertThatThrownBy(() -> testInstance.doGetBinder(setColumn))
				.isInstanceOf(BindingException.class)
				.hasMessage("No binder found for type j.u.Set");
	}
}