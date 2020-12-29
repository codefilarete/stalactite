package org.gama.stalactite.query.builder;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class FromBuilderTest {
	
	public static Object[][] testToSQL_data() {
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
		
		return new Object[][] {
				// testing syntax with Table API
				// 1 join, with or without alias
				{ new From().innerJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto inner join Tata on Toto.id = Tata.id" },
				{ new From().innerJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to inner join Tata as ta on to.id = ta.id" },
				{ new From().leftOuterJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto left outer join Tata on Toto.id = Tata.id" },
				{ new From().leftOuterJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to left outer join Tata as ta on to.id = ta.id" },
				{ new From().rightOuterJoin(tableToto, tableTata, "Toto.id = Tata.id"), "Toto right outer join Tata on Toto.id = Tata.id" },
				{ new From().rightOuterJoin(tableToto, "to", tableTata, "ta", "to.id = ta.id"), "Toto as to right outer join Tata as ta on to.id = ta.id" },
				// 2 joins, with or without alias
				{ new From().innerJoin(tableToto, tableTata, "Toto.a = Tata.a").innerJoin(tableToto, tableTutu, "Toto.b = Tutu.b"),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From().innerJoin(tableToto, "to", tableTata, "ta", "to.a = ta.a").innerJoin(tableToto, "to", tableTutu, null, "to.b = Tutu.b"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From().innerJoin(tableToto, "to", tableTata, null, "to.a = Tata.a").innerJoin(tableToto, "to", tableTutu, "tu", "to.b = tu.b"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				
				// testing syntax with Column API
				// 1 join, with or without alias
				{ new From().innerJoin(colTotoA, colTataA), "Toto inner join Tata on Toto.a = Tata.a" },
				{ new From().innerJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to inner join Tata as ta on to.a = ta.a" },
				{ new From().leftOuterJoin(colTotoA, colTataA), "Toto left outer join Tata on Toto.a = Tata.a" },
				{ new From().leftOuterJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to left outer join Tata as ta on to.a = ta.a" },
				{ new From().rightOuterJoin(colTotoA, colTataA), "Toto right outer join Tata on Toto.a = Tata.a" },
				{ new From().rightOuterJoin(colTotoA, colTataA)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"), "Toto as to right outer join Tata as ta on to.a = ta.a" },
				
				// 2 joins, with or without alias
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu on to.b = Tutu.b" },
				{ new From().innerJoin(colTotoA, colTataA).crossJoin(tableToto2).innerJoin(colToto2B, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta"),
						"Toto as to inner join Tata as ta on to.a = ta.a cross join Toto2 inner join Tutu on Toto2.b = Tutu.b" },
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTutuB.getTable(), "tu"),
						"Toto as to inner join Tata on to.a = Tata.a inner join Tutu as tu on to.b = tu.b" },
				{ new From().innerJoin(colTotoA, colTataA).innerJoin(colTotoB, colTutuB).innerJoin(colTutuB, colToto2B)
						.setAlias(colTotoA.getTable(), "to")
						.setAlias(colTataA.getTable(), "ta")
						.setAlias(colTutuB.getTable(), "tu"),
						"Toto as to inner join Tata as ta on to.a = ta.a inner join Tutu as tu on to.b = tu.b inner join Toto2 on tu.b = Toto2.b" },

				// mix with Table and Column
				{ new From().innerJoin(tableToto, tableTata, "Toto.a = Tata.a").innerJoin(colTotoB, colTutuB),
						"Toto inner join Tata on Toto.a = Tata.a inner join Tutu on Toto.b = Tutu.b" },
				
				// testing syntax with cross join
				{ new From().add(tableToto),
						"Toto" },
				{ new From().add(tableToto).crossJoin(tableTata),
						"Toto cross join Tata" },
				{ new From().innerJoin(tableToto, tableTata, "id = id"),
						"Toto inner join Tata on id = id" },
				{ new From().add(tableToto, "to").crossJoin(tableTata).innerJoin(tableToto, "to", tableTutu, "", "id = id"),
						"Toto as to cross join Tata inner join Tutu on id = id" },
				{ new From().add(tableToto, "to").crossJoin(tableTata).innerJoin(colTotoA, colTutuA)
						.setAlias(colTutuA.getTable(), ""),
						"Toto as to cross join Tata inner join Tutu on to.a = Tutu.a" },
				{ new From().add(tableToto).leftOuterJoin(tableToto, tableTata, "id = id"),
						"Toto left outer join Tata on id = id" },
				{ new From().add(tableToto).crossJoin(tableTata).leftOuterJoin(tableTata, tableTutu, "id = id"),
						"Toto cross join Tata left outer join Tutu on id = id" },
				{ new From().add(tableToto).rightOuterJoin(tableToto, tableTata, "id = id"),
						"Toto right outer join Tata on id = id" },
				{ new From().add(tableToto).crossJoin(tableTata).rightOuterJoin(tableToto, tableTutu, "id = id"),
						"Toto cross join Tata right outer join Tutu on id = id" },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testToSQL_data")
	public void testToSQL(From from, String expected) {
		FromBuilder testInstance = new FromBuilder(from);
		assertEquals(expected, testInstance.toSQL());
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
		
		From from = new From()
				.innerJoin(colToto1A, colTataA)
				.innerJoin(colToto1B, colTutuB)
				.innerJoin(colTutuB, colToto2B)
				.setAlias(tableToto1, "to")
				.setAlias(tableTata, "ta")
				.setAlias(tableTutu, "tu");
		FromBuilder testInstance = new FromBuilder(from);
		from.getTableAliases().put(tableToto2, "toChanged");
		from.getTableAliases().put(tableTutu, "tuChanged");
		assertEquals("Toto as to inner join Tata as ta on to.a = ta.a"
						+ " inner join Tutu as tuChanged on to.b = tuChanged.b"
						+ " inner join Toto as toChanged on tuChanged.b = toChanged.b",
				testInstance.toSQL());
		
	}
}