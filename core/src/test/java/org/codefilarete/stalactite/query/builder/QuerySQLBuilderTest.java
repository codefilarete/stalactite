package org.codefilarete.stalactite.query.builder;

import java.util.Arrays;
import java.util.Map;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.builder.PseudoTableSQLBuilderFactory.PseudoTableSQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.gt;
import static org.codefilarete.stalactite.query.model.Operators.lt;
import static org.codefilarete.stalactite.query.model.Operators.sum;
import static org.codefilarete.stalactite.query.model.OrderByChain.Order.ASC;
import static org.codefilarete.stalactite.query.model.OrderByChain.Order.DESC;
import static org.codefilarete.stalactite.query.model.QueryEase.select;

/**
 * @author Guillaume Mary
 */
class QuerySQLBuilderTest {
	
	private final Dialect dialect = new Dialect();
	
	public static Object[][] toSQL() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.addColumn("a", String.class);
		Column colTotoB = tableToto.addColumn("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
		
		Query query1 = select(colTotoA, colTotoB).from(tableToto).getQuery();
		Query query2 = select(colTataA, colTataB).from(tableTata).getQuery();
		Union union = new Union(query1, query2);
		
		return new Object[][] {
				{ select(colTotoA, colTotoB).from(tableToto),
					"select Toto.a, Toto.b from Toto" },
				{ select(colTotoA, colTotoB).distinct().from(tableToto),
					"select distinct Toto.a, Toto.b from Toto" },
				{ select(colTotoA, colTotoB).from(tableToto, "t"),
					"select t.a, t.b from Toto as t" },
				{ select(colTotoA, colTotoB, colTataA, colTataB).from(tableToto, "to").crossJoin(tableTata),
					"select to.a, to.b, Tata.a, Tata.b from Toto as to cross join Tata" },
				{ select(colTotoA, colTotoB, colTataA, colTataB).from(tableToto, "to").crossJoin(tableTata, "ta"),
					"select to.a, to.b, ta.a, ta.b from Toto as to cross join Tata as ta" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y"),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y" },
				{ select(colTotoA, colTataB).from(colTotoA, colTataB)
						.setAlias(colTotoA.getTable(), "toto")
						.setAlias(colTataB.getTable(), "tata"),
						"select toto.a, tata.b from Toto as toto inner join Tata as tata on toto.a = tata.b" },
				{ select(colTotoA, colTataB).from(query1.asPseudoTable()),
						"select Toto.a, Tata.b from (select Toto.a, Toto.b from Toto)" },
				{ select(colTotoA, colTataB).from(query1.asPseudoTable("a")),
						"select Toto.a, Tata.b from (select Toto.a, Toto.b from Toto) as a" },
				{ select(colTotoA, colTataB).from(union.asPseudoTable()),
						"select Toto.a, Tata.b from (select Toto.a, Toto.b from Toto union all select Tata.a, Tata.b from Tata)" },
				{ select(colTotoA, colTataB).from(union.asPseudoTable("a")),
						"select Toto.a, Tata.b from (select Toto.a, Toto.b from Toto union all select Tata.a, Tata.b from Tata) as a" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1"),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.groupBy(colTotoB),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.groupBy(colTotoB).having("sum(", colTotoB, ") > 1"),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b having sum(Toto.b) > 1" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1").and(colTataA, "= 4")
						.groupBy(colTotoB).add(colTataB).having("sum(", colTotoB, ") > 1").and("count(id) = 0"),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 and Tata.a = 4 group by Toto.b, Tata.b having sum(Toto.b) > 1 and count(id) = 0" },
				// Order by test
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy(colTotoA, ASC),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by Toto.a asc" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy(colTotoA, DESC),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by Toto.a desc" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy("titi", ASC),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by titi asc" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy("titi", DESC),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by titi desc" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy(colTotoA, colTataB),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by Toto.a, Tata.b" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy(colTotoA, colTataB).add("titi", ASC),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by Toto.a, Tata.b, titi asc" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.orderBy("titi").add(colTotoA, colTataB),
						"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 order by titi, Toto.a, Tata.b" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.groupBy(colTotoB).orderBy(colTotoB),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b order by Toto.b" },
				{ select(colTotoA, colTataB).from(tableToto, tableTata, "x = y").where(colTotoB, "= 1")
						.groupBy(colTotoB).having("sum(", colTotoB, ") > 1").orderBy(colTotoB),
					"select Toto.a, Tata.b from Toto inner join Tata on x = y where Toto.b = 1 group by Toto.b having sum(Toto.b) > 1 order by Toto.b" },
				// limit test
				{ select(colTotoA, colTataB).from(tableToto).limit(2),
						"select Toto.a, Tata.b from Toto limit 2" },
				{ select(colTotoA, colTataB).from(tableToto).limit(2).unionAll(select(colTotoA, colTataB).from(tableToto).limit(2)),
						"select Toto.a, Tata.b from Toto limit 2 union all select Toto.a, Tata.b from Toto limit 2" },
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, "= 1").limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = 1 limit 2" },
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, "= 1").orderBy(colTotoA).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = 1 order by Toto.a limit 2" },
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, "= 1").groupBy(colTotoA).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = 1 group by Toto.a limit 2" },
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, "= 1").groupBy(colTotoA)
						.having(sum(colTotoB), " > 1").limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = 1 group by Toto.a having sum(Toto.b) > 1 limit 2" },
				{ select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, "= 1").groupBy(colTotoA)
						.having(sum(colTotoB), " > 1").limit(2),
						"select T.a, Tata.b from Toto as T where T.b = 1 group by T.a having sum(T.b) > 1 limit 2" },
				{ select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, "= 1").unionAll( 
						select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, "= 1")),
						"select T.a, Tata.b from Toto as T where T.b = 1"
								+ " union all"
								+ " select T.a, Tata.b from Toto as T where T.b = 1" },
				// sub-select in joins
				{ select(colTotoA, colTotoB)
						.from(tableToto, "T")
						.innerJoin(select(tableTata.getColumns())
									.from(tableTata, "TATA"), "subTATA", "Toto.a = subTATA.a"),
						"select T.a, T.b from Toto as T inner join (select TATA.a, TATA.b from Tata as TATA) as subTATA on Toto.a = subTATA.a" },
				{ select(colTotoA, colTotoB)
						.from(tableToto, "T")
						.leftOuterJoin(select(tableTata.getColumns())
									.from(tableTata, "TATA"), "subTATA", "Toto.a = subTATA.a"),
						"select T.a, T.b from Toto as T left outer join (select TATA.a, TATA.b from Tata as TATA) as subTATA on Toto.a = subTATA.a" },
				{ select(colTotoA, colTotoB)
						.from(tableToto, "T")
						.rightOuterJoin(select(tableTata.getColumns())
									.from(tableTata, "TATA"), "subTATA", "Toto.a = subTATA.a"),
						"select T.a, T.b from Toto as T right outer join (select TATA.a, TATA.b from Tata as TATA) as subTATA on Toto.a = subTATA.a" }
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL")
	public void toSQL(QueryProvider<?> queryProvider, String expected) {
		SQLBuilder testInstance = sqlBuilder(queryProvider.getQuery());
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
	
	/**
	 * Creates a {@link QuerySQLBuilder} or a {@link PseudoTableSQLBuilder} depending on given {@link QueryStatement} type.
	 * Throws an exception if {@link QueryStatement} is neither a {@link Query} nor a {@link Union}.
	 *
	 * @param queryStatement a {@link Query} or a {@link Union}
	 * @return a {@link QuerySQLBuilder} or a {@link PseudoTableSQLBuilder}
	 */
	private SQLBuilder sqlBuilder(QueryStatement queryStatement) {
		QuerySQLBuilderFactory querySQLBuilderFactory = dialect.getQuerySQLBuilderFactory();
		if (queryStatement instanceof Query) {
			return querySQLBuilderFactory.queryBuilder((Query) queryStatement);
		} else if (queryStatement instanceof Union) {
			return new PseudoTableSQLBuilder(queryStatement, querySQLBuilderFactory);
		} else {
			throw new UnsupportedOperationException(Reflections.toString(queryStatement.getClass()) + " has no supported SQL generator");
		}
	}
	
	private PreparedSQLBuilder preparedSQLBuilder(QueryStatement queryStatement) {
		QuerySQLBuilderFactory querySQLBuilderFactory = dialect.getQuerySQLBuilderFactory();
		if (queryStatement instanceof Query) {
			return querySQLBuilderFactory.queryBuilder((Query) queryStatement);
		} else if (queryStatement instanceof Union) {
			return dialect.getUnionSQLBuilderFactory().pseudoTableBuilder(queryStatement, querySQLBuilderFactory);
		} else {
			throw new UnsupportedOperationException(Reflections.toString(queryStatement.getClass()) + " has no supported SQL generator");
		}
	}
	
	public static Object[][] toPreparedSQL() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.addColumn("a", String.class);
		Column colTotoB = tableToto.addColumn("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
		
		TupleIn tupleIn = new TupleIn(new Column[] { colTotoA, colTataB }, Arrays.asList(
				new Object[] { "John", "Doe" },
				new Object[] { "Jane", null }
		));
		
		return new Object[][] {
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, eq(1)).groupBy(colTotoA)
						.having("sum(", colTotoB, ") ", gt(1)).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = ? group by Toto.a having sum(Toto.b) > ? limit ?",
						Maps.forHashMap(Integer.class, Object.class).add(1, 1).add(2, 1).add(3, 2) },
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, eq(1)).groupBy(colTotoA)
						.having(sum(colTotoB), lt(1)).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = ? group by Toto.a having sum(Toto.b)< ? limit ?",
						Maps.forHashMap(Integer.class, Object.class).add(1, 1).add(2, 1).add(3, 2) },
				{ select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, eq(1)).groupBy(colTotoA)
						.having(sum(colTotoB), gt(1)).limit(2),
						"select T.a, Tata.b from Toto as T where T.b = ? group by T.a having sum(T.b)> ? limit ?",
						Maps.forHashMap(Integer.class, Object.class).add(1, 1).add(2, 1).add(3, 2) },
				{ select(colTotoA, colTataB).from(tableToto, "T").where(tupleIn).groupBy(colTotoA)
						.having(sum(colTotoB), gt(1)).limit(2),
						"select T.a, Tata.b from Toto as T where (T.a, Tata.b) in ((?, ?), (?, ?)) group by T.a having sum(T.b)> ? limit ?",
						Maps.forHashMap(Integer.class, Object.class).add(1, "John").add(2, "Doe").add(3, "Jane").add(4, null).add(5, 1).add(6, 2) },
				{ select(colTotoA, colTataB).from(tableToto).innerJoin(colTotoA, colTataA).setAlias(tableTata, "x").where(colTataB, eq(1)),
						"select Toto.a, x.b from Toto inner join Tata as x on Toto.a = x.a where x.b = ?",
						Maps.forHashMap(Integer.class, Object.class).add(1, 1) },
				{ select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, eq(11)).unionAll(
						select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, eq(22))),
						"(select T.a, Tata.b from Toto as T where T.b = ?)"
								+ " union all"
								+ " (select T.a, Tata.b from Toto as T where T.b = ?)",
						Maps.forHashMap(Integer.class, Object.class).add(1, 11).add(2, 22)},
		};
	}
	
	@ParameterizedTest
	@MethodSource("toPreparedSQL")
	public void toPreparedSQL(QueryProvider<Query> queryProvider, String expectedPreparedStatement, Map<Integer, Object> expectedValues) {
		PreparedSQLBuilder testInstance = preparedSQLBuilder(queryProvider.getQuery());
		PreparedSQL preparedSQL = testInstance.toPreparedSQL();
		assertThat(preparedSQL.getSQL()).isEqualTo(expectedPreparedStatement);
		assertThat(preparedSQL.getValues()).isEqualTo(expectedValues);
	}
}