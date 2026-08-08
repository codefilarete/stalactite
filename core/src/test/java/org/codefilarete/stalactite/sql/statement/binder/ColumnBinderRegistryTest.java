package org.codefilarete.stalactite.sql.statement.binder;

import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
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
		assertThatThrownBy(() -> testInstance.register(nameColumn, (ParameterBinder<? extends Object>) DefaultParameterBinders.INTEGER_BINDER))
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
	
	@Test
	void copyConstructor_modificationsOnCloneDontImpactOriginalInstance() {
		ColumnBinderRegistry original = new ColumnBinderRegistry();
		Table table = new Table("toto");
		Column nameColumn = table.addColumn("name", String.class);
		original.register(nameColumn, DefaultParameterBinders.STRING_BINDER);
		
		ColumnBinderRegistry clone = new ColumnBinderRegistry(original);
		
		// modifying the clone with a new column binder and a new class (type) binder, both unknown from the original
		Column idColumn = table.addColumn("id", DummyType1.class);
		clone.register(idColumn, DefaultParameterBinders.STRING_BINDER);
		clone.register(DummyType2.class, (ParameterBinder) DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		
		// the clone sees its own modifications ...
		assertThat(clone.getBinder(idColumn)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(clone.getBinder(DummyType2.class)).isEqualTo(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		// ... as well as registrations made on the original before cloning
		assertThat(clone.getBinder(nameColumn)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		
		// but the original instance is left untouched by clone's modifications
		assertThatThrownBy(() -> original.getBinder(idColumn))
				.isInstanceOf(BindingException.class)
				.hasMessage("No binder found for type " + Reflections.toString(DummyType1.class));
		assertThatThrownBy(() -> original.getBinder(DummyType2.class))
				.isInstanceOf(BindingException.class)
				.hasMessage("No binder found for type " + Reflections.toString(DummyType2.class));
	}
	
	private static class DummyType1 {
		
	}
	
	private static class DummyType2 {
		
	}
}
