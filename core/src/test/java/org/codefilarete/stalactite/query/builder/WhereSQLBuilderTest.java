package org.codefilarete.stalactite.query.builder;

import java.util.Map;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory.OperatorSQLBuilder;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.Collections.EMPTY_MAP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.between;
import static org.codefilarete.stalactite.query.model.Operators.endsWith;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.gt;
import static org.codefilarete.stalactite.query.model.Operators.gteq;
import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.stalactite.query.model.Operators.like;
import static org.codefilarete.stalactite.query.model.Operators.lt;
import static org.codefilarete.stalactite.query.model.Operators.lteq;
import static org.codefilarete.stalactite.query.model.Operators.not;
import static org.codefilarete.stalactite.query.model.Operators.startsWith;
import static org.codefilarete.stalactite.query.model.QueryEase.filter;
import static org.codefilarete.stalactite.query.model.QueryEase.where;

/**
 * @author Guillaume Mary
 */
public class WhereSQLBuilderTest {
	
	public static Object[][] toSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colA = tableToto.addColumn("a", String.class);
		Column colB = tableToto.addColumn("b", String.class);
		Map<Table, String> tableAliases = Maps.asMap(tableToto, "t");
		
		return new Object[][] {
				// test simple Where
				{ where(colA, eq("28")), EMPTY_MAP, "Toto.a = '28'" },
				{ where(colA, eq("28")).and(colB, eq("1")), EMPTY_MAP, "Toto.a = '28' and Toto.b = '1'" },
				{ where(colA, eq("28")).and(filter(colB, eq("1"))), EMPTY_MAP, "Toto.a = '28' and (Toto.b = '1')" },
				{ where(colA, eq("o'clock")).and(filter(colB, eq("o'clock"))), EMPTY_MAP, "Toto.a = 'o''clock' and (Toto.b = 'o''clock')" },
				{ where(colA, like("o'clock")).and(filter(colB, like("o'clock"))), EMPTY_MAP, "Toto.a like 'o''clock' and (Toto.b like 'o''clock')" },
				{ where(colA, "= 28").and(colB, "= 1"), EMPTY_MAP, "Toto.a = 28 and Toto.b = 1" },
				{ where(colA, "= 28").or(colB, "= 1"), EMPTY_MAP, "Toto.a = 28 or Toto.b = 1" },
				// test simple Where + alias
				{ where(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ where(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test Criteria
				{ filter(colA, "= 28").and(colB, "= 1"), tableAliases, "t.a = 28 and t.b = 1" },
				{ filter(colA, "= 28").or(colB, "= 1"), tableAliases, "t.a = 28 or t.b = 1" },
				// test Where composed of Criteria
				{ where(colA, "= 28").and(filter(colB, "= 1")), tableAliases, "t.a = 28 and (t.b = 1)" },
				{ where(colA, "= 28").or(filter(colB, "= 1")), tableAliases, "t.a = 28 or (t.b = 1)" },
				// test more complex Where composed of Criteria
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1)" },
				{ where(colA, "= 28").or(filter(colB, "= 1").or(colB, "< -1")), tableAliases, "t.a = 28 or (t.b = 1 or t.b < -1)" },
				// test Where composed of Criteria composed of Criteria
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1").and(filter(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 and (t.a = 42))" },
				{ where(colA, "= 28").or(filter(colB, "= 1").and(colB, "< -1").or(filter(colA, "= 42"))), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1 or (t.a = 42))" },
				// test Where composed of String Criteria
				{ where(colA, "= 28").or(filter("t.b = 1").and("t.b < -1")), tableAliases, "t.a = 28 or (t.b = 1 and t.b < -1)" },
				{ where(colA, "= 28").or(filter( "t.b = 1").or("t.b < -1")), tableAliases, "t.a = 28 or (t.b = 1 or t.b < -1)" },
				{ where(colA, "= 28").or(filter( "t.b = 1").or(colB, eq(colA))), tableAliases, "t.a = 28 or (t.b = 1 or t.b = t.a)" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL_data")
	public void toSQL(CriteriaChain where, Map<Table, String> tableAliases, String expected) {
		DMLNameProvider dmlNameProvider = new DMLNameProvider(tableAliases);
		FunctionSQLBuilder functionSQLBuilder = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		WhereSQLBuilder testInstance = new WhereSQLBuilder(where, dmlNameProvider, new ColumnBinderRegistry(), new OperatorSQLBuilder(functionSQLBuilder), functionSQLBuilder);
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
	
	public static Object[][] toPreparedSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colA = tableToto.addColumn("a", String.class);
		Column colB = tableToto.addColumn("b", String.class);
		Map<Table, String> tableAliases = Maps.asMap(tableToto, "t");
		
		return new Object[][] {
				// test simple Where
				{ where(colA, eq(28)).and(colB, eq(1)), EMPTY_MAP,
						"Toto.a = ? and Toto.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				{ where(colA, eq(28)).or(colB, eq(1)), EMPTY_MAP,
						"Toto.a = ? or Toto.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				// test simple Where + alias
				{ where(colA, eq(28)).and(colB, eq(1)), tableAliases,
						"t.a = ? and t.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				{ where(colA, eq(28)).or(colB, eq(1)), tableAliases,
						"t.a = ? or t.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				// test Criteria
				{ filter(colA, eq(28)).and(colB, eq(1)), tableAliases,
						"t.a = ? and t.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				{ filter(colA, eq(28)).or(colB, eq(1)), tableAliases,
						"t.a = ? or t.b = ?", Maps.asHashMap(1, 28).add(2, 1) },
				// test Where composed of Criteria
				{ where(colA, eq(28)).and(filter(colB, eq(1))), tableAliases,
						"t.a = ? and (t.b = ?)", Maps.asHashMap(1, 28).add(2, 1) },
				{ where(colA, eq(28)).or(filter(colB, eq(1))), tableAliases,
						"t.a = ? or (t.b = ?)", Maps.asHashMap(1, 28).add(2, 1) },
				// test more complex Where composed of Criteria
				{ where(colA, eq(28)).or(filter(colB, eq(1)).and(colB, lt(-1))), tableAliases,
						"t.a = ? or (t.b = ? and t.b < ?)", Maps.asHashMap(1, 28).add(2, 1).add(3, -1) },
				{ where(colA, eq(28)).or(filter(colB, eq(1)).or(colB, lt(-1))), tableAliases,
						"t.a = ? or (t.b = ? or t.b < ?)", Maps.asHashMap(1, 28).add(2, 1).add(3, -1) },
				// test Where composed of Criteria composed of Criteria
				{ where(colA, eq(28)).or(filter(colB, eq(1)).and(colB, lt(-1)).and(filter(colA, eq(42)))), tableAliases,
						"t.a = ? or (t.b = ? and t.b < ? and (t.a = ?))", Maps.asHashMap(1, 28).add(2, 1).add(3, -1).add(4, 42) },
				{ where(colA, eq(28)).or(filter(colB, eq(1)).and(colB, lt(-1)).or(filter(colA, eq(42)))), tableAliases,
						"t.a = ? or (t.b = ? and t.b < ? or (t.a = ?))", Maps.asHashMap(1, 28).add(2, 1).add(3, -1).add(4, 42) },
				// test Where composed of String Criteria
				{ where(colA, eq(28)).or(filter(colB, eq(1)).and(colB, lt(-1))), tableAliases,
						"t.a = ? or (t.b = ? and t.b < ?)", Maps.asHashMap(1, 28).add(2, 1).add(3, -1) },
				{ where(colA, eq(28)).or(filter(colB, eq(1)).or(colB, lt(-1))), tableAliases,
						"t.a = ? or (t.b = ? or t.b < ?)", Maps.asHashMap(1, 28).add(2, 1).add(3, -1) },
				{ where(colA, eq(28)), EMPTY_MAP,
						"Toto.a = ?", Maps.asHashMap(1, 28) },
				{ where(colA, between(10, 20)), EMPTY_MAP,
						"Toto.a between ? and ?", Maps.asHashMap(1, 10).add(2, 20) },
				{ where(colA, eq(null)), EMPTY_MAP,
						"Toto.a is null", EMPTY_MAP },
				{ where(colA, between("1", null)), EMPTY_MAP,
						"Toto.a > ?", Maps.asHashMap(1, "1")},
				{ where(colA, between(null, "2")), EMPTY_MAP,
						"Toto.a < ?", Maps.asHashMap(1, "2")},
				{ where(colA, not(between("1", "2"))), EMPTY_MAP,
						"Toto.a not between ? and ?", Maps.asHashMap(1, "1").add(2, "2")},
				{ where(colA, not(between(null, null))), EMPTY_MAP,
						"Toto.a is not null", EMPTY_MAP},
				{ where(colA, not(between("1", null))), EMPTY_MAP,
						"Toto.a <= ?", Maps.asHashMap(1, "1")},
				{ where(colA, not(between(null, "2"))), EMPTY_MAP,
						"Toto.a >= ?", Maps.asHashMap(1, "2")},
				{ where(colA, lt("1")), EMPTY_MAP,
						"Toto.a < ?", Maps.asHashMap(1, "1")},
				{ where(colA, gt("1")), EMPTY_MAP,
						"Toto.a > ?", Maps.asHashMap(1, "1")},
				{ where(colA, lteq("1")), EMPTY_MAP,
						"Toto.a <= ?", Maps.asHashMap(1, "1")},
				{ where(colA, gteq("1")), EMPTY_MAP,
						"Toto.a >= ?", Maps.asHashMap(1, "1")},
				{ where(colA, not(lteq("1"))), EMPTY_MAP,
						"Toto.a > ?", Maps.asHashMap(1, "1")},
				{ where(colA, not(gteq("1"))), EMPTY_MAP,
						"Toto.a < ?", Maps.asHashMap(1, "1")},
				{ where(colA, like("x")), EMPTY_MAP,
						"Toto.a like ?", Maps.asHashMap(1, "x")},
				{ where(colA, startsWith("x")), EMPTY_MAP,
						"Toto.a like ?", Maps.asHashMap(1, "x%")},
				{ where(colA, endsWith("x")), EMPTY_MAP,
						"Toto.a like ?", Maps.asHashMap(1, "%x")},
				{ where(colA, not(like("x"))), EMPTY_MAP,
						"Toto.a not like ?", Maps.asHashMap(1, "x")},
				{ where(colA, not(startsWith("x"))), EMPTY_MAP,
						"Toto.a not like ?", Maps.asHashMap(1, "x%")},
				{ where(colA, not(endsWith("x"))), EMPTY_MAP,
						"Toto.a not like ?", Maps.asHashMap(1, "%x")},
				{ where(colA, in("x")), EMPTY_MAP,
						"Toto.a in (?)", Maps.asHashMap(1, "x")},
				{ where(colA, in("x", "y")), EMPTY_MAP,
						"Toto.a in (?, ?)", Maps.asHashMap(1, "x").add(2, "y")},
				{ where(colA, not(in("x"))), EMPTY_MAP,
						"Toto.a not in (?)", Maps.asHashMap(1, "x")},
				{ where(colA, in((Iterable) null)), EMPTY_MAP,
						"Toto.a is null", EMPTY_MAP},
				{ where(colA, in(Arrays.asSet("x", "y"))), EMPTY_MAP,
						"Toto.a in (?, ?)", Maps.asHashMap(1, "x").add(2, "y")},
		};
	}
	
	@ParameterizedTest
	@MethodSource("toPreparedSQL_data")
	public void toPreparedSQL(CriteriaChain where, Map<Table, String> tableAliases,
							  String expectedPreparedStatement, Map<Integer, Object> expectedValues) {
		DMLNameProvider dmlNameProvider = new DMLNameProvider(tableAliases);
		FunctionSQLBuilder functionSQLBuilder = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		WhereSQLBuilder testInstance = new WhereSQLBuilder(where, dmlNameProvider, new ColumnBinderRegistry(), new OperatorSQLBuilder(functionSQLBuilder), functionSQLBuilder);
		PreparedSQL preparedSQL = testInstance.toPreparedSQL();
		assertThat(preparedSQL.getSQL()).isEqualTo(expectedPreparedStatement);
		assertThat(preparedSQL.getValues()).isEqualTo(expectedValues);
	}
	
}