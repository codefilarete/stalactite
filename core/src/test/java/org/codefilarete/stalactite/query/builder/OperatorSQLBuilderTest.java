package org.codefilarete.stalactite.query.builder;

import java.util.Collections;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.StringAppender;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.*;

/**
 * @author Guillaume Mary
 */
public class OperatorSQLBuilderTest {
	
	private final DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void cat_nullValue_isTransformedToIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.cat(new ConditionalOperator() {
			@Override
			public boolean isNull() {
				return true;
			}
		}, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
	}
	
	@Test
	public void catNullValue() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catNullValue(false, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
		
		result = new StringAppender();
		testInstance.catNullValue(true, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is not null");
	}
	
	@Test
	public void catIsNull() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catIsNull(new IsNull(), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
		
		result = new StringAppender();
		testInstance.catIsNull(not(new IsNull()), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is not null");
		
		// nothing happens on a value set on is null
		result = new StringAppender();
		IsNull isNull = new IsNull();
		isNull.setValue(42);
		testInstance.catIsNull(isNull, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
	}
	
	@Test
	public void catLike() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLike(new Like("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like 'a'");
		
		result = new StringAppender();
		testInstance.catLike(contains("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like '%a%'");
		
		result = new StringAppender();
		testInstance.catLike(startsWith("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like 'a%'");
		
		result = new StringAppender();
		testInstance.catLike(endsWith("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("like '%a'");	}
	
	@Test
	public void catIn() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catIn(in("a", "b"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("in ('a', 'b')");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.catIn(in(), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("in ()");
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.cat(in((Object) null), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("in (null)");
	}
	
	@Test
	public void catBetween() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catBetween(between(1, 2), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("between 1 and 2");
		
		result = new StringAppender();
		testInstance.catBetween(between(1, null), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
		
		result = new StringAppender();
		testInstance.catBetween(between(null, 2), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 2");
	}
	
	@Test
	public void catGreater() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catGreater(gt(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
		
		result = new StringAppender();
		testInstance.catGreater(not(gt(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("<= 1");

		result = new StringAppender();
		testInstance.catGreater(gteq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo(">= 1");
		
		result = new StringAppender();
		testInstance.catGreater(not(gteq(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 1");
	}
	
	@Test
	public void catLower() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLower(lt(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("< 1");
		
		result = new StringAppender();
		testInstance.catLower(not(lt(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo(">= 1");
		
		result = new StringAppender();
		testInstance.catLower(lteq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("<= 1");
		
		result = new StringAppender();
		testInstance.catLower(not(lteq(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("> 1");
	}
	
	@Test
	public void catEquals() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(eq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= 1");
	}
	
	@Test
	public void catEquals_column() {
		OperatorSQLBuilder testInstance = new OperatorSQLBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= Toto.a");
	}
}