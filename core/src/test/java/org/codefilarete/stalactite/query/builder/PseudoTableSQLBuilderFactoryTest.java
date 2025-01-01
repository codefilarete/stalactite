package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.builder.PseudoTableSQLBuilderFactory.PseudoTableSQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.QueryEase.select;

class PseudoTableSQLBuilderFactoryTest {
	
	private final Dialect dialect = new DefaultDialect();
	private final QuerySQLBuilderFactory querySQLBuilderFactory = dialect.getQuerySQLBuilderFactory();
	
	public static Object[][] toSQL() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.addColumn("a", String.class);
		Column colTotoB = tableToto.addColumn("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
		
		Query query1 = select(colTotoA, colTotoB).from(tableToto).getQuery();
		Query query2 = select(colTataA, colTataB).from(tableTata).getQuery();
		
		return new Object[][] {
				{ query1,
						"select Toto.a, Toto.b from Toto" },
				{ new Union(query1, query2),
						"select Toto.a, Toto.b from Toto union all select Tata.a, Tata.b from Tata" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL")
	public void toSQL(QueryProvider<?> queryProvider, String expected) {
		PseudoTableSQLBuilder testInstance = new PseudoTableSQLBuilder(queryProvider.getQuery(), querySQLBuilderFactory);
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
}