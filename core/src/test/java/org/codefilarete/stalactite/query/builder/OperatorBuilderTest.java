package org.codefilarete.stalactite.query.builder;

import java.util.Collections;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.between;
import static org.codefilarete.stalactite.query.model.Operators.contains;
import static org.codefilarete.stalactite.query.model.Operators.count;
import static org.codefilarete.stalactite.query.model.Operators.endsWith;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.gt;
import static org.codefilarete.stalactite.query.model.Operators.gteq;
import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.stalactite.query.model.Operators.lt;
import static org.codefilarete.stalactite.query.model.Operators.lteq;
import static org.codefilarete.stalactite.query.model.Operators.max;
import static org.codefilarete.stalactite.query.model.Operators.min;
import static org.codefilarete.stalactite.query.model.Operators.not;
import static org.codefilarete.stalactite.query.model.Operators.startsWith;
import static org.codefilarete.stalactite.query.model.Operators.sum;

/**
 * @author Guillaume Mary
 */
public class OperatorBuilderTest {
	
	private DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void cat_nullValue_isTransformedToIsNull() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.cat(new AbstractRelationalOperator() {
			@Override
			public boolean isNull() {
				return true;
			}
		}, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
	}
	
	@Test
	public void catNullValue() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catNullValue(false, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is null");
		
		result = new StringAppender();
		testInstance.catNullValue(true, new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("is not null");
	}
	
	@Test
	public void catIsNull() {
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
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
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(eq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= 1");
	}
	
	@Test
	public void catEquals_column() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertThat(result.toString()).isEqualTo("= Toto.a");
	}
	
	@Test
	public void catSum() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catSum(sum(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("sum(Toto.a)");
	}
	
	@Test
	public void catCount() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catCount(count(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("count(Toto.a)");
	}
	
	@Test
	public void catMin() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMin(min(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("min(Toto.a)");
	}
	
	@Test
	public void catMax() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMax(max(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertThat(result.toString()).isEqualTo("max(Toto.a)");
	}
}