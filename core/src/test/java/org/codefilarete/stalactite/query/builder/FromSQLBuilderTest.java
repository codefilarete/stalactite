package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.builder.FromSQLBuilderFactory.FromSQLBuilder;
import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoColumn;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.QueryEase.select;

/**
 * @author Guillaume Mary
 */
public class FromSQLBuilderTest {
	
	public static Object[][] toSQL_data() {
		Table tableToto = new Table(null, "Toto");
		Column colTotoA = tableToto.addColumn("a", String.class);
		Column colTotoB = tableToto.addColumn("b", String.class);
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
		Table tableTutu = new Table(null, "Tutu");
		Column colTutuA = tableTutu.addColumn("a", String.class);
		Column colTutuB = tableTutu.addColumn("b", String.class);
		
		Table tableToto2 = new Table(null, "Toto2");
		Column colToto2A = tableToto2.addColumn("a", String.class);
		Column colToto2B = tableToto2.addColumn("b", String.class);
		
		Union union = select(colTotoA, colTotoB).from(tableToto, "T1").where(colTotoB, "= 1").unionAll(
				select(colTotoA, colTotoB).from(tableToto, "T2").where(colTotoB, "= 2"));
		
		union.registerColumn("a", String.class);
		union.registerColumn("b", String.class);
		PseudoTable unionAsPseudoTable = union.asPseudoTable("Tutu");
		PseudoColumn<String> colUnionAsPseudoTableA = (PseudoColumn<String>) unionAsPseudoTable.mapColumnsOnName().get("a");
		
		PseudoTable pseudoTable = select(tableTata.getColumns())
				.from(tableTata).getQuery().asPseudoTable("myPseudoTable");
		PseudoColumn<String> colPseudoTableA = (PseudoColumn<String>) pseudoTable.mapColumnsOnName().get("a");
		
		return new Object[][] {
				// testing syntax with Table API
				// 1 join, with or without alias
				{ new From(tableToto).innerJoin(tableTata, "Toto.id = Tata.id"), "Toto inner join Tata on Toto.id = Tata.id" },
				{ new From(tableToto, "to").innerJoin(tableTata, "ta", "to.id = ta.id"), "Toto as to inner join Tata as ta on to.id = ta.id" },
				{ new From(tableToto).leftOuterJoin(tableTata, "Toto.id = Tata.id"), "Toto left outer join Tata on Toto.id = Tata.id" },
				{ new From(tableToto, "to").leftOuterJoin(tableTata, "ta", "to.id = ta.id"), "Toto as to left outer join Tata as ta on to.id = ta.id" },
				{ new From(tableToto).rightOuterJoin(tableTata, "Toto.id = Tata.id"), "Toto right outer join Tata on Toto.id = Tata.id" },
				{ new From(tableToto, "to").rightOuterJoin(tableTata, "ta", "to.id = ta.id"), "Toto as to right outer join Tata as ta on to.id = ta.id" },
				// 2 joins, with or without alias
				{ new From(tableToto).innerJoin(tableTata, "Toto.a = Tata.a").innerJoin(tableTutu, "Toto.b = Tutu.b"),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From(tableToto, "to").innerJoin(tableTata, "ta", "to.a = ta.a").innerJoin(tableTutu, null, "to.b = Tutu.b"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From(tableToto, "to").innerJoin(tableTata, null, "to.a = Tata.a").innerJoin(tableTutu, "tu", "to.b = tu.b"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				
				// testing syntax with Column API
				// 1 join, with or without alias
				{ new From(tableToto).innerJoin(colTotoA, colTataA), "Toto inner join Tata on Toto.a = Tata.a" },
				{ new From(tableToto).innerJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to inner join Tata as ta on to.a = ta.a" },
				{ new From(tableToto).leftOuterJoin(colTotoA, colTataA), "Toto left outer join Tata on Toto.a = Tata.a" },
				{ new From(tableToto).leftOuterJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to left outer join Tata as ta on to.a = ta.a" },
				{ new From(tableToto).rightOuterJoin(colTotoA, colTataA), "Toto right outer join Tata on Toto.a = Tata.a" },
				{ new From(tableToto).rightOuterJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to right outer join Tata as ta on to.a = ta.a" },
				
				// 2 joins, with or without alias
				{ new From(tableToto).innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From(tableToto).innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From(tableToto).innerJoin(colTotoA, colTataA).crossJoin(tableToto2).innerJoin(colToto2B, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"),
						"Toto as to inner join Tata as ta on to.a = ta.a cross join Toto2 inner join Tutu on Toto2.b = Tutu.b" },
				{ new From(tableToto).innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTutuB.getTable(), "tu"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				{ new From(tableToto).innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB).innerJoin(colTutuB, colToto2B)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta")
						.setAlias(colTutuB.getTable(), "tu"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu as tu on to.b = tu.b inner join Toto2 on tu.b = Toto2.b" },

				// mix with Table and Column
				{ new From(tableToto).innerJoin(tableTata, "Toto.a = Tata.a").innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				
				// testing syntax with cross join
				{ new From(tableToto),
						"Toto" },
				{ new From(tableToto).crossJoin(tableTata),
						"Toto cross join Tata" },
				{ new From(tableToto).innerJoin(tableTata, "id = id"),
						"Toto inner join Tata on id = id" },
				{ new From(tableToto, "to").crossJoin(tableTata).innerJoin(tableTutu, "", "id = id"),
						"Toto as to cross join Tata inner join Tutu on id = id" },
				{ new From(tableToto, "to").crossJoin(tableTata).innerJoin(colTotoA, colTutuA)
						.setAlias(colTutuA.getTable(), ""),
						"Toto as to cross join Tata inner join Tutu on to.a = Tutu.a" },
				{ new From(tableToto).leftOuterJoin(tableTata, "id = id"),
						"Toto left outer join Tata on id = id" },
				{ new From(tableToto).crossJoin(tableTata).leftOuterJoin(tableTutu, "id = id"),
						"Toto cross join Tata left outer join Tutu on id = id" },
				{ new From(tableToto).rightOuterJoin(tableTata, "id = id"),
						"Toto right outer join Tata on id = id" },
				{ new From(tableToto).crossJoin(tableTata).rightOuterJoin(tableTutu, "id = id"),
						"Toto cross join Tata right outer join Tutu on id = id" },
				
				// with pseudo table
				{ new From(tableToto).leftOuterJoin(union.asPseudoTable("Tutu"), "z = y"),
						"Toto left outer join (select T1.a, T1.b from Toto as T1 where T1.b = 1 union all select T2.a, T2.b from Toto as T2 where T2.b = 2) as Tutu on z = y" },
				{ new From(tableToto).leftOuterJoin(colTotoA, colUnionAsPseudoTableA),
						"Toto left outer join (select T1.a, T1.b from Toto as T1 where T1.b = 1 union all select T2.a, T2.b from Toto as T2 where T2.b = 2) as Tutu on Toto.a = Tutu.a" },
				{ new From(tableToto).leftOuterJoin(colTotoA, colPseudoTableA),
						"Toto left outer join (select Tata.a, Tata.b from Tata) as myPseudoTable on Toto.a = myPseudoTable.a" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL_data")
	public void toSQL(From from, String expected) {
		FromSQLBuilder testInstance = new FromSQLBuilder(from,
				new DMLNameProvider(from.getTableAliases()::get),
				new QuerySQLBuilderFactory(new DefaultTypeMapping(), DMLNameProvider::new, new ColumnBinderRegistry())
		);
		assertThat(testInstance.toSQL()).isEqualTo(expected);
	}
	
	@Test
	void toSQL_supportsSeveralSameTable() {
		Table tableToto1 = new Table(null, "Toto");
		Column colToto1A = tableToto1.addColumn("a", String.class);
		Column colToto1B = tableToto1.addColumn("b", String.class);
		// making a clone to test table of same name several time in From (goal of the test) 
		Table tableToto2 = new Table(null, tableToto1.getName());
		Column colToto2A = tableToto2.addColumn(colToto1A.getName(), colToto1A.getJavaType());
		Column colToto2B = tableToto2.addColumn(colToto1B.getName(), colToto1B.getJavaType());
		
		Table tableTata = new Table(null, "Tata");
		Column colTataA = tableTata.addColumn("a", String.class);
		Column colTataB = tableTata.addColumn("b", String.class);
		Table tableTutu = new Table(null, "Tutu");
		Column colTutuA = tableTutu.addColumn("a", String.class);
		Column colTutuB = tableTutu.addColumn("b", String.class);
		
		From from = new From(tableToto1)
				.innerJoin(colToto1A, colTataA)
				.innerJoin(colToto1B, colTutuB)
				.innerJoin(colTutuB, colToto2B)
				.setAlias(tableToto1, "to")
				.setAlias(tableTata, "ta")
				.setAlias(tableTutu, "tu");
		FromSQLBuilder testInstance = new FromSQLBuilder(from,
				new DMLNameProvider(from.getTableAliases()::get),
				new QuerySQLBuilderFactory(new DefaultTypeMapping(), DMLNameProvider::new, new ColumnBinderRegistry())
		);
		from.getTableAliases().put(tableToto2, "toChanged");
		from.getTableAliases().put(tableTutu, "tuChanged");
		assertThat(testInstance.toSQL()).isEqualTo("Toto as to inner join Tata as ta on to.a = ta.a"
				+ " inner join Tutu as tuChanged on to.b = tuChanged.b"
				+ " inner join Toto as toChanged on tuChanged.b = toChanged.b");
		
	}
}