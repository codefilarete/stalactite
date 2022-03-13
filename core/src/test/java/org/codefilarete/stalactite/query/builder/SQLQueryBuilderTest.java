package org.codefilarete.stalactite.query.builder;

import java.util.Map;

import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.gt;
import static org.codefilarete.stalactite.query.model.Operators.sum;
import static org.codefilarete.stalactite.query.model.OrderByChain.Order.ASC;
import static org.codefilarete.stalactite.query.model.OrderByChain.Order.DESC;
import static org.codefilarete.stalactite.query.model.QueryEase.select;

/**
 * @author Guillaume Mary
 */
public class SQLQueryBuilderTest {
	
	public static Object[][] testToSQL_data() {
		final Table tableToto = new Table(null, "Toto");
		final Column colTotoA = tableToto.addColumn("a", String.class);
		final Column colTotoB = tableToto.addColumn("b", String.class);
		final Table tableTata = new Table(null, "Tata");
		final Column colTataA = tableTata.addColumn("a", String.class);
		final Column colTataB = tableTata.addColumn("b", String.class);
		
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
		};
	}
	
	@ParameterizedTest
	@MethodSource("testToSQL_data")
	public void testToSQL(QueryProvider queryProvider, String expected) {
		SQLQueryBuilder testInstance = new SQLQueryBuilder(queryProvider.getQuery());
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
	
	public static Object[][] testToPreparedSQL_data() {
		final Table tableToto = new Table(null, "Toto");
		final Column colTotoA = tableToto.addColumn("a", String.class);
		final Column colTotoB = tableToto.addColumn("b", String.class);
		final Table tableTata = new Table(null, "Tata");
		final Column colTataA = tableTata.addColumn("a", String.class);
		final Column colTataB = tableTata.addColumn("b", String.class);
		
		return new Object[][] {
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, eq(1)).groupBy(colTotoA)
						.having("sum(", colTotoB, ") ", gt(1)).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = ? group by Toto.a having sum(Toto.b) > ? limit ?",
						Maps.asHashMap(1, 1).add(2, 1).add(3, 2)},
				{ select(colTotoA, colTataB).from(tableToto).where(colTotoB, eq(1)).groupBy(colTotoA)
						.having(sum(colTotoB), gt(1)).limit(2),
						"select Toto.a, Tata.b from Toto where Toto.b = ? group by Toto.a having sum(Toto.b)> ? limit ?",
						Maps.asHashMap(1, 1).add(2, 1).add(3, 2)},
				{ select(colTotoA, colTataB).from(tableToto, "T").where(colTotoB, eq(1)).groupBy(colTotoA)
						.having(sum(colTotoB), gt(1)).limit(2),
						"select T.a, Tata.b from Toto as T where T.b = ? group by T.a having sum(T.b)> ? limit ?",
						Maps.asHashMap(1, 1).add(2, 1).add(3, 2)},
		};
	}
	
	@ParameterizedTest
	@MethodSource("testToPreparedSQL_data")
	public void testToPreparedSQL(QueryProvider queryProvider,
								  String expectedPreparedStatement, Map<Integer, Object> expectedValues) {
		SQLQueryBuilder testInstance = new SQLQueryBuilder(queryProvider);
		ColumnBinderRegistry parameterBinderRegistry = new ColumnBinderRegistry();
		PreparedSQL preparedSQL = testInstance.toPreparedSQL(parameterBinderRegistry);
		assertThat(preparedSQL.getSQL()).isEqualTo(expectedPreparedStatement);
		assertThat(preparedSQL.getValues()).isEqualTo(expectedValues);
	}
}