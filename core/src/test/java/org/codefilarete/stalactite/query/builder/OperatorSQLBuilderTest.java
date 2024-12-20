package org.codefilarete.stalactite.query.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.Coalesce;
import org.codefilarete.stalactite.query.model.operator.DateFormat;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.LowerCase;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.query.model.operator.UpperCase;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.between;
import static org.codefilarete.stalactite.query.model.Operators.contains;
import static org.codefilarete.stalactite.query.model.Operators.endsWith;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.gt;
import static org.codefilarete.stalactite.query.model.Operators.gteq;
import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.stalactite.query.model.Operators.lowerCase;
import static org.codefilarete.stalactite.query.model.Operators.lt;
import static org.codefilarete.stalactite.query.model.Operators.lteq;
import static org.codefilarete.stalactite.query.model.Operators.max;
import static org.codefilarete.stalactite.query.model.Operators.not;
import static org.codefilarete.stalactite.query.model.Operators.startsWith;
import static org.codefilarete.stalactite.query.model.Operators.trim;
import static org.codefilarete.stalactite.query.model.Operators.upperCase;

/**
 * @author Guillaume Mary
 */
class OperatorSQLBuilderTest {
	
	private final DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void cat_nullValue_isTransformedToIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.cat(new ConditionalOperator<Object, Object>() {
			@Override
			public void setValue(Variable<Object> value) {
				
			}
			
			@Override
			public boolean isNull() {
				return true;
			}
		}, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
	}
	
	@Test
	public void catNullValue() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catNullValue(false, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
		
		result = new StringAppender();
		testInstance.catNullValue(true, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is not null");
	}
	
	@Test
	public void catIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catIsNull(new IsNull(), new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
		
		result = new StringAppender();
		testInstance.catIsNull(not(new IsNull()), new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is not null");
		
		// nothing happens on a value set on is null
		result = new StringAppender();
		IsNull isNull = new IsNull();
		isNull.setValue(42);
		testInstance.catIsNull(isNull, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
	}
	
	@Test
	public void catLike() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catLike(new Like("a"), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like 'a'");
		
		result = new StringAppender();
		testInstance.catLike(contains("a"), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like '%a%'");
		
		result = new StringAppender();
		testInstance.catLike(startsWith("a"), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like 'a%'");
		
		result = new StringAppender();
		testInstance.catLike(endsWith("a"), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like '%a'");
	}
	
	@Test
	public void catLike_withFunction() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catLike(new Like<>(new LowerCase<>("a")), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower('a')");

		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>("a"), true, true), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower('%a%')");

		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>("a"), false, true), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower('a%')");

		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>("a"), true, false), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower('%a')");
		
		// checking deep composition
		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>("a"))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower(upper('a'))");
		
		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>(new DateFormat(new SelectableString<>("\"2018-09-24\"", CharSequence.class), "%D %b %Y")))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower(upper(date_format(\"2018-09-24\", '%D %b %Y')))");
		
		result = new StringAppender();
		testInstance.catLike(contains(lowerCase(upperCase(trim("a")))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower(upper(trim('%a%')))");
		
		// The following makes no sense and is only made to document the behavior
		Table tableToto = new Table("Toto");
		Column<?, String> colA = tableToto.addColumn("a", String.class);
		Column<?, String> colB = tableToto.addColumn("b", String.class);
		result = new StringAppender();
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>(new Coalesce<>(colA, new Cast<>(colB, String.class)))), true, true), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like lower(upper(coalesce(Toto.a, cast(Toto.b as varchar))))");
	}
	
	@Test
	public void catIn() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catIn(in("a", "b"), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("in ('a', 'b')");
		
		result = new StringAppender();
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		Column<?, Integer> colB = tableToto.addColumn("b", Integer.class);
		testInstance.catIn(in(max(colA), max(colB)), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("in (max(Toto.a), max(Toto.b))");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.catIn(in(), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("in ()");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.cat(in((Object) null), new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("in (null)");
	}
	
	@Test
	public void catIn_tupled() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		Table dummyTable = new Table("dummyTable");
		Column firstName = dummyTable.addColumn("firstName", String.class);
		Column lastName = dummyTable.addColumn("lastName", String.class);
		
		TupleIn tupleIn = new TupleIn(new Column[] { firstName, lastName }, Arrays.asList(
				new Object[] { "John", "Doe" },
				new Object[] { "Jane", "Doe" },
				new Object[] { "Paul", "Smith" }));
		testInstance.cat(tupleIn, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (('John', 'Doe'), ('Jane', 'Doe'), ('Paul', 'Smith'))");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, Collections.emptyList());
		testInstance.cat(tupleIn, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in ()");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, (List) null);
		testInstance.cat(tupleIn, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (null, null)");
		
		result = new StringAppender();
		List<Object[]> input = new ArrayList<>();
		input.add(new Object[] { "John", null });
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, input);
		testInstance.cat(tupleIn, new StringSQLAppender(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (('John', null))");
	}
	
	@Test
	public void catBetween() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catBetween(between(1, 2), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("between 1 and 2");
		
		result = new StringAppender();
		testInstance.catBetween(between(1, null), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
		
		result = new StringAppender();
		testInstance.catBetween(between(null, 2), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 2");
	}
	
	@Test
	public void catGreater() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catGreater(gt(1), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
		
		result = new StringAppender();
		testInstance.catGreater(not(gt(1)), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("<= 1");

		result = new StringAppender();
		testInstance.catGreater(gteq(1), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo(">= 1");
		
		result = new StringAppender();
		testInstance.catGreater(not(gteq(1)), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 1");
	}
	
	@Test
	public void catLower() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catLower(lt(1), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 1");
		
		result = new StringAppender();
		testInstance.catLower(not(lt(1)), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo(">= 1");
		
		result = new StringAppender();
		testInstance.catLower(lteq(1), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("<= 1");
		
		result = new StringAppender();
		testInstance.catLower(not(lteq(1)), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
	}
	
	@Test
	public void catEquals() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(eq(1), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= 1");
	}
	
	@Test
	public void catEquals_withFunction() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(new Equals<>(new LowerCase<>("a")), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= lower('a')");

		// checking deep composition
		result = new StringAppender();
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>("a"))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= lower(upper('a'))");
		
		result = new StringAppender();
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>(new DateFormat(new SelectableString<>("\"2018-09-24\"", CharSequence.class), "%D %b %Y")))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= lower(upper(date_format(\"2018-09-24\", '%D %b %Y')))");
		
		// The following makes no sense and is only made to document the behavior
		Table tableToto = new Table("Toto");
		Column<?, String> colA = tableToto.addColumn("a", String.class);
		Column<?, String> colB = tableToto.addColumn("b", String.class);
		result = new StringAppender();
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>(new Coalesce<>(colA, new Cast<>(colB, String.class))))), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= lower(upper(coalesce(Toto.a, cast(Toto.b as varchar))))");
	}
	
	@Test
	public void catEquals_column() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()));
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), new StringSQLAppender(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= Toto.a");
	}
}