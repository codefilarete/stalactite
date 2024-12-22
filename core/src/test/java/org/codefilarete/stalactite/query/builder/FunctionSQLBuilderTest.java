package org.codefilarete.stalactite.query.builder;

import java.util.Collections;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.model.operator.Cast;
import org.codefilarete.stalactite.query.model.operator.Coalesce;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.query.model.operator.Max;
import org.codefilarete.stalactite.query.model.operator.Min;
import org.codefilarete.stalactite.query.model.operator.Sum;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.cast;
import static org.codefilarete.stalactite.query.model.Operators.substring;

class FunctionSQLBuilderTest {
	
	private final DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void catCount() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Count(colA), result);
		assertThat(result.getSQL()).isEqualTo("count(Toto.a)");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(new Count(colA).distinct(), result);
		assertThat(result.getSQL()).isEqualTo("count(distinct Toto.a)");
	}
	
	@Test
	public void catMin() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Min<>(colA), result);
		assertThat(result.getSQL()).isEqualTo("min(Toto.a)");
	}
	
	@Test
	public void catMax() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Max<>(colA), result);
		assertThat(result.getSQL()).isEqualTo("max(Toto.a)");
	}
	
	@Test
	public void catSum() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.cat(new Sum<>(colA), result);
		assertThat(result.getSQL()).isEqualTo("sum(Toto.a)");
	}
	
	@Test
	public void catCast() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.cat(new Cast<>("toto", Integer.class), result);
		assertThat(result.getSQL()).isEqualTo("cast(toto as integer)");
	}
	
	@Test
	public void catCast_null() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		testInstance.cat(new Cast<>((String) null, Integer.class), result);
		assertThat(result.getSQL()).isEqualTo("cast(null as integer)");
	}
	
	@Test
	public void catCast_function() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class, 128);
		
		testInstance.cat(new Cast<>(new Max<>(colA), Integer.class), result);
		assertThat(result.getSQL()).isEqualTo("cast(max(Toto.a) as integer)");
	}
	
	@Test
	public void catCast_typeWithSize() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", String.class, 128);
		
		testInstance.cat(new Cast<>(colA, String.class, 128), result);
		assertThat(result.getSQL()).isEqualTo("cast(Toto.a as varchar(128))");
	}
	
	@Test
	public void coalesce() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", String.class);
		Column colB = tableToto.addColumn("b", String.class);
		
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(new Coalesce<>(colA, colB), result);
		assertThat(result.getSQL()).isEqualTo("coalesce(Toto.a, Toto.b)");
	}
	
	@Test
	public void combining_functions() {
		FunctionSQLBuilder testInstance = new FunctionSQLBuilder(dmlNameProvider, new DefaultTypeMapping());
		StringSQLAppender result;
		
		Table tableToto = new Table("Toto");
		Column<?, Integer> colA = tableToto.addColumn("a", Integer.class);
		Column<?, String> colB = tableToto.addColumn("b", String.class);
		
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(new Coalesce<>(colA, new Cast<>(colB, String.class)), result);
		assertThat(result.getSQL()).isEqualTo("coalesce(Toto.a, cast(Toto.b as varchar))");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(new Max<>(new Coalesce<>(colA, new Cast<>(colB, String.class))), result);
		assertThat(result.getSQL()).isEqualTo("max(coalesce(Toto.a, cast(Toto.b as varchar)))");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(substring(colB, 2, 30), result);
		assertThat(result.getSQL()).isEqualTo("substring(Toto.b, 2, 30)");
		
		result = new StringSQLAppender(dmlNameProvider);
		testInstance.cat(substring(cast(colA, String.class), 2, 30), result);
		assertThat(result.getSQL()).isEqualTo("substring(cast(Toto.a as varchar), 2, 30)");
	}
}