package org.codefilarete.stalactite.sql.statement.binder;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
class ParameterBinderRegistryTest {
	
	@Test
	void getBinder_notFound() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		assertThat(testInstance.getBinder(String.class)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
	}
	
	@Test
	void getBinder_notRegistered_throwsException() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		assertThatThrownBy(() -> testInstance.getBinder(Object.class))
			.isInstanceOf(BindingException.class)
			.hasMessage("No binder found for type j.l.Object");
	}
	
	@Test
	void getBinder_findCompliantBinder() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		// just to be sure that nobody corrupted this test by adding StringBuilder in the default registry
		assertThat(testInstance.getBinderPerType().containsKey(StringBuilder.class)).isFalse();
		
		NullAwareParameterBinder<CharSequence> expectedBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, String::toString, CharSequence::toString));
		testInstance.register(CharSequence.class, expectedBinder);
		assertThat(testInstance.getBinder(StringBuilder.class)).isEqualTo(expectedBinder);
		assertThat(testInstance.getBinderPerType().containsKey(StringBuilder.class)).isTrue();
	}
	
	@Test
	void getBinder_findCompliantBinder_enum() throws SQLException {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		// just to be sure that nobody corrupted this test by adding TimeUnit in the default registry
		assertThat(testInstance.getBinderPerType().containsKey(TimeUnit.class)).isFalse();
		
		// because enum binders are dynamically produced we don't have to register it nor can't check their presence by reference checking
		// so we ask to read some data and see if it's an enum
		ParameterBinder<TimeUnit> timeUnitBinder = testInstance.getBinder(TimeUnit.class);
		InMemoryResultSet fakeResultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
				.add("X", TimeUnit.SECONDS.ordinal()),	// we store our enum value eas ordinal because default binder is an ordinal one
				Maps.forHashMap(String.class, Object.class)
				.add("X", null)	// check for null
		));
		fakeResultSet.next();	// we have to "start" the ResultSet
		assertThat(timeUnitBinder.get(fakeResultSet, "X")).isEqualTo(TimeUnit.SECONDS);
		fakeResultSet.next();
		assertThat(timeUnitBinder.get(fakeResultSet, "X")).isNull();
		assertThat(testInstance.getBinderPerType().containsKey(TimeUnit.class)).isTrue();
	}
	
	@Test
	void getBinder_multipleCompliantBindersFound_throwsException() {
		ParameterBinderRegistry testInstance = new ParameterBinderRegistry();
		NullAwareParameterBinder<CharSequence> charSequenceBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, String::toString, CharSequence::toString));
		testInstance.register(CharSequence.class, charSequenceBinder);
		NullAwareParameterBinder<Serializable> serializableBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				DefaultParameterBinders.STRING_BINDER, Serializable::toString, Serializable::toString));
		testInstance.register(Serializable.class, serializableBinder);
		assertThatThrownBy(() -> testInstance.getBinder(StringBuilder.class))
				.isInstanceOf(BindingException.class)
				.hasMessage("Multiple binders found for j.l.StringBuilder, please register one for any of : [j.l"
						+ ".CharSequence, j.i.Serializable]");
	}
}