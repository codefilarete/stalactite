package org.codefilarete.stalactite.query.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder.LikePatternAppender;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.Operators;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.query.api.Variable;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.Coalesce;
import org.codefilarete.stalactite.query.model.operator.DateFormat;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.LowerCase;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.query.model.operator.UpperCase;
import org.codefilarete.stalactite.sql.QuerySQLBuilderFactoryBuilder;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.Operators.avg;
import static org.codefilarete.stalactite.query.Operators.between;
import static org.codefilarete.stalactite.query.Operators.contains;
import static org.codefilarete.stalactite.query.Operators.endsWith;
import static org.codefilarete.stalactite.query.Operators.eq;
import static org.codefilarete.stalactite.query.Operators.gt;
import static org.codefilarete.stalactite.query.Operators.gteq;
import static org.codefilarete.stalactite.query.Operators.in;
import static org.codefilarete.stalactite.query.Operators.lowerCase;
import static org.codefilarete.stalactite.query.Operators.lt;
import static org.codefilarete.stalactite.query.Operators.lteq;
import static org.codefilarete.stalactite.query.Operators.max;
import static org.codefilarete.stalactite.query.Operators.not;
import static org.codefilarete.stalactite.query.Operators.startsWith;
import static org.codefilarete.stalactite.query.Operators.trim;
import static org.codefilarete.stalactite.query.Operators.upperCase;
import static org.codefilarete.stalactite.query.api.OrderByChain.Order.DESC;
import static org.codefilarete.stalactite.query.model.QueryEase.select;

/**
 * @author Guillaume Mary
 */
class OperatorSQLBuilderTest {
	
	private final DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void cat_nullValue_isTransformedToIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.cat(new ConditionalOperator<Object, Object>() {
			@Override
			public void setValue(Variable<Object> value) {
				
			}
			
			@Override
			public boolean isNull() {
				return true;
			}
		}, result);
		assertThat(result.getSQL()).isEqualTo("is null");
	}
	
	@Test
	public void catNullValue() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catNullValue(false, result);
		assertThat(result.getSQL()).isEqualTo("is null");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catNullValue(true, result);
		assertThat(result.getSQL()).isEqualTo("is not null");
	}
	
	@Test
	public void catIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catIsNull(new IsNull<>(), result);
		assertThat(result.getSQL()).isEqualTo("is null");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catIsNull(Operators.<IsNull<Object>, Object, Object>not(new IsNull<>()), result);
		assertThat(result.getSQL()).isEqualTo("is not null");
		
		// nothing happens on a value set on is null
		result = new StringSQLAppender(dmlNameProvider);
		IsNull<Integer> isNull = new IsNull<>();
		isNull.setValue(42);
		testInstance.catIsNull(isNull, result);
		assertThat(result.getSQL()).isEqualTo("is null");
	}
	
	@Test
	public void catLike() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catLike(new Like("a"), result, null);
		assertThat(result.getSQL()).isEqualTo("like 'a'");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(contains("a"), result, null);
		assertThat(result.getSQL()).isEqualTo("like '%a%'");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(startsWith("a"), result, null);
		assertThat(result.getSQL()).isEqualTo("like 'a%'");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(endsWith("a"), result, null);
		assertThat(result.getSQL()).isEqualTo("like '%a'");
	}
	
	@Test
	public void catLike_withFunction() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catLike(new Like<>(new LowerCase<>("a")), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower('a')");

		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>("a"), true, true), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower('%a%')");

		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>("a"), false, true), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower('a%')");

		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>("a"), true, false), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower('%a')");
		
		// checking deep composition
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>("a"))), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower(upper('a'))");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>(new DateFormat(new SimpleSelectable<>("\"2018-09-24\"", CharSequence.class), "%D %b %Y")))), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower(upper(date_format(\"2018-09-24\", '%D %b %Y')))");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(contains(lowerCase(upperCase(trim("a")))), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower(upper(trim('%a%')))");
		
		// The following makes no sense and is only made to document the behavior
		Table tableToto = new Table("Toto");
		Column<?, String> colA = tableToto.addColumn("a", String.class);
		Column<?, String> colB = tableToto.addColumn("b", String.class);
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLike(new Like<>(new LowerCase<>(new UpperCase<>(new Coalesce<>(colA, new Cast<>(colB, String.class)))), true, true), result, null);
		assertThat(result.getSQL()).isEqualTo("like lower(upper(coalesce(Toto.a, cast(Toto.b as varchar(255)))))");
	}
	
	@Test
	public void catIn() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catIn(in("a", "b"), result, null);
		assertThat(result.getSQL()).isEqualTo("in ('a', 'b')");
		
		result = new StringSQLAppender(dmlNameProvider);
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		Column<?, Integer> colB = tableToto.addColumn("b", Integer.class);
		testInstance.catIn(in(max(colA), max(colB)), result, null);
		assertThat(result.getSQL()).isEqualTo("in (max(Toto.a), max(Toto.b))");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catIn(in(), result, null);
		assertThat(result.getSQL()).isEqualTo("in ()");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(in((Object) null), result);
		assertThat(result.getSQL()).isEqualTo("in (null)");
	}
	
	@Test
	public void catIn_tupled() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table dummyTable = new Table("dummyTable");
		Column firstName = dummyTable.addColumn("firstName", String.class);
		Column lastName = dummyTable.addColumn("lastName", String.class);
		
		TupleIn tupleIn = new TupleIn(new Column[] { firstName, lastName }, Arrays.asList(
				new Object[] { "John", "Doe" },
				new Object[] { "Jane", "Doe" },
				new Object[] { "Paul", "Smith" }));
		testInstance.cat(tupleIn, result);
		assertThat(result.getSQL()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (('John', 'Doe'), ('Jane', 'Doe'), ('Paul', 'Smith'))");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringSQLAppender(dmlNameProvider);
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, Collections.emptyList());
		testInstance.cat(tupleIn, result);
		assertThat(result.getSQL()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in ()");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringSQLAppender(dmlNameProvider);
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, (List) null);
		testInstance.cat(tupleIn, result);
		assertThat(result.getSQL()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (null, null)");
		
		result = new StringSQLAppender(dmlNameProvider);
		List<Object[]> input = new ArrayList<>();
		input.add(new Object[] { "John", null });
		tupleIn = new TupleIn(new Column[] { firstName, lastName }, input);
		testInstance.cat(tupleIn, result);
		assertThat(result.getSQL()).isEqualTo("(dummyTable.firstName, dummyTable.lastName) in (('John', null))");
	}
	
	@Test
	public void catInSubQuery() {
		QuerySQLBuilderFactory sqlBuilderFactory = new QuerySQLBuilderFactoryBuilder(DMLNameProvider::new, new ColumnBinderRegistry(), new DefaultTypeMapping()).build();
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), sqlBuilderFactory);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table<?> reviewTable = new Table("Review");
		Column<?, Long> restaurantIdColumn = reviewTable.addColumn("restaurantId", Long.class);
		Column<?, Long> ratingColumn = reviewTable.addColumn("rating", Long.class);
		testInstance.catInSubQuery(Operators.in(
						select(restaurantIdColumn)
								.from(reviewTable)
								.groupBy(restaurantIdColumn)
								.orderBy(avg(ratingColumn), DESC)
								.limit(5)), result);
		assertThat(result.getSQL()).isEqualTo("in (select Review.restaurantId from Review group by Review.restaurantId order by avg(Review.rating) desc limit 5)")	;
	}
	
	@Test
	public void catBetween() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catBetween(between(1, 2), result, null);
		assertThat(result.getSQL()).isEqualTo("between 1 and 2");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catBetween(between(1, null), result, null);
		assertThat(result.getSQL()).isEqualTo("> 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catBetween(between(null, 2), result, null);
		assertThat(result.getSQL()).isEqualTo("< 2");
	}
	
	@Test
	public void catGreater() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catGreater(gt(1), result, null);
		assertThat(result.getSQL()).isEqualTo("> 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catGreater(not(gt(1)), result, null);
		assertThat(result.getSQL()).isEqualTo("<= 1");

		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catGreater(gteq(1), result, null);
		assertThat(result.getSQL()).isEqualTo(">= 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catGreater(not(gteq(1)), result, null);
		assertThat(result.getSQL()).isEqualTo("< 1");
	}
	
	@Test
	public void catLower() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catLower(lt(1), result, null);
		assertThat(result.getSQL()).isEqualTo("< 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLower(not(lt(1)), result, null);
		assertThat(result.getSQL()).isEqualTo(">= 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLower(lteq(1), result, null);
		assertThat(result.getSQL()).isEqualTo("<= 1");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catLower(not(lteq(1)), result, null);
		assertThat(result.getSQL()).isEqualTo("> 1");
	}
	
	@Test
	public void catEquals() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catEquals(eq(1), result, null);
		assertThat(result.getSQL()).isEqualTo("= 1");
	}
	
	@Test
	public void catEquals_withFunction() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.catEquals(new Equals<>(new LowerCase<>("a")), result, null);
		assertThat(result.getSQL()).isEqualTo("= lower('a')");

		// checking deep composition
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>("a"))), result, null);
		assertThat(result.getSQL()).isEqualTo("= lower(upper('a'))");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>(new DateFormat(new SimpleSelectable<>("\"2018-09-24\"", CharSequence.class), "%D %b %Y")))), result, null);
		assertThat(result.getSQL()).isEqualTo("= lower(upper(date_format(\"2018-09-24\", '%D %b %Y')))");
		
		// The following makes no sense and is only made to document the behavior
		Table tableToto = new Table("Toto");
		Column<?, String> colA = tableToto.addColumn("a", String.class);
		Column<?, String> colB = tableToto.addColumn("b", String.class);
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.catEquals(new Equals<>(new LowerCase<>(new UpperCase<>(new Coalesce<>(colA, new Cast<>(colB, String.class))))), result, null);
		assertThat(result.getSQL()).isEqualTo("= lower(upper(coalesce(Toto.a, cast(Toto.b as varchar(255)))))");
	}
	
	@Test
	public void catEquals_column() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder(new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping()), null);
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), result, null);
		assertThat(result.getSQL()).isEqualTo("= Toto.a");
	}
	
	@Nested
	class LikePatternAppenderTest {
		
		@Test
		public void likePattern() {
			LikePatternAppender testInstance = new LikePatternAppender(new Like<>(false, true), new StringSQLAppender(dmlNameProvider));
			testInstance.cat("select * from Toto where a like(")
					.catValue("me")
					.cat(")");
			
			String preparedSQL = testInstance.getSQL();
			assertThat(preparedSQL).isEqualTo("select * from Toto where a like('me%')");
		}
		
		@Test
		void newSubPart() {
			Table<?> totoTable = new Table<>("Toto");
			LikePatternAppender testInstance = new LikePatternAppender(new Like<>(false, true), new StringSQLAppender(new DMLNameProvider(k -> "Tutu")));
			testInstance.cat("select * from ")
					.newSubPart(new DMLNameProvider(k -> "Tata"))
					.cat("(select * from ")
					.catTable(totoTable)
					// we check "like" print in subpart to see if subpart follows parent algorithm
					.cat(" where a like(").catValue("me").cat("))")
					.close()
					.cat(", ")
					.newSubPart(new DMLNameProvider(k -> "Titi"))
					.cat("(select * from ")
					.catTable(totoTable)
					.cat(")")
					.close()
					.cat(", ")
					.catTable(totoTable)
					.cat(" where a like(")
					// we check "like" print even if it's a stupid test since in real life "like" operator is hardly reused in conditions
					.catValue("me").cat(")")
			;
			
			String preparedSQL = testInstance.getSQL();
			// Remember: DMLNameProvider is only used for alias finding there by SQL builder, SQL appenders don't use it, so "Toto" is always printed
			// even if we gave aliases to the test instance
			assertThat(preparedSQL).isEqualTo("select * from (select * from Toto where a like('me%')), (select * from Toto), Toto where a like('me%')");
		}
	}
}
