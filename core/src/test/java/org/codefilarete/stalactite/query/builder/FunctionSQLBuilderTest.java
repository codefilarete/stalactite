package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.operator.*;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.StringAppender;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class FunctionSQLBuilderTest {
	
	private final DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void catCount() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Count<>(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("count(Toto.a)");
	}
	
	@Test
	public void catMin() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Min<>(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("min(Toto.a)");
	}
	
	@Test
	public void catMax() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Max<>(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("max(Toto.a)");
	}
	
	@Test
	public void catSum() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Sum<>(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("sum(Toto.a)");
	}
	
	@Test
	public void catCast() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.cat(new Cast<>("toto", Integer.class), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("cast('toto' as integer)");
	}
	
	@Test
	public void catCast_null() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.cat(new Cast<>((String) null, Integer.class), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("cast(null as integer)");
	}
	
	@Test
	public void catCast_function() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class, 128);
		
		testInstance.cat(new Cast<>(new Max<>(colA), Integer.class), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("cast(max(Toto.a) as integer)");
	}
	
	@Test
	public void catCast_typeWithSize() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", String.class, 128);
		
		testInstance.cat(new Cast<>(colA, String.class, 128), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("cast(Toto.a as varchar(128))");
	}
	
}