package org.gama.stalactite.query.builder;

import java.util.Collections;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.query.model.Operators.between;
import static org.gama.stalactite.query.model.Operators.contains;
import static org.gama.stalactite.query.model.Operators.count;
import static org.gama.stalactite.query.model.Operators.endsWith;
import static org.gama.stalactite.query.model.Operators.eq;
import static org.gama.stalactite.query.model.Operators.gt;
import static org.gama.stalactite.query.model.Operators.gteq;
import static org.gama.stalactite.query.model.Operators.in;
import static org.gama.stalactite.query.model.Operators.lt;
import static org.gama.stalactite.query.model.Operators.lteq;
import static org.gama.stalactite.query.model.Operators.max;
import static org.gama.stalactite.query.model.Operators.min;
import static org.gama.stalactite.query.model.Operators.not;
import static org.gama.stalactite.query.model.Operators.startsWith;
import static org.gama.stalactite.query.model.Operators.sum;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
		assertEquals("is null", result.toString());
	}
	
	@Test
	public void catNullValue() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catNullValue(false, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is null", result.toString());
		
		result = new StringAppender();
		testInstance.catNullValue(true, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is not null", result.toString());
	}
	
	@Test
	public void catIsNull() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catIsNull(new IsNull(), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is null", result.toString());
		
		result = new StringAppender();
		testInstance.catIsNull(not(new IsNull()), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is not null", result.toString());
		
		// nothing happens on a value set on is null
		result = new StringAppender();
		IsNull isNull = new IsNull();
		isNull.setValue(42);
		testInstance.catIsNull(isNull, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is null", result.toString());
	}
	
	@Test
	public void catLike() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLike(new Like("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("like 'a'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(contains("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("like '%a%'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(startsWith("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("like 'a%'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(endsWith("a"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("like '%a'", result.toString());	}
	
	@Test
	public void catIn() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catIn(in("a", "b"), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("in ('a', 'b')", result.toString());
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.catIn(in(), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("in ()", result.toString());
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.cat(in((Object) null), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("in (null)", result.toString());
	}
	
	@Test
	public void catBetween() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catBetween(between(1, 2), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("between 1 and 2", result.toString());
		
		result = new StringAppender();
		testInstance.catBetween(between(1, null), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("> 1", result.toString());
		
		result = new StringAppender();
		testInstance.catBetween(between(null, 2), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("< 2", result.toString());
	}
	
	@Test
	public void catGreater() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catGreater(gt(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("> 1", result.toString());
		
		result = new StringAppender();
		testInstance.catGreater(not(gt(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("<= 1", result.toString());

		result = new StringAppender();
		testInstance.catGreater(gteq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals(">= 1", result.toString());
		
		result = new StringAppender();
		testInstance.catGreater(not(gteq(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("< 1", result.toString());
	}
	
	@Test
	public void catLower() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLower(lt(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("< 1", result.toString());
		
		result = new StringAppender();
		testInstance.catLower(not(lt(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals(">= 1", result.toString());
		
		result = new StringAppender();
		testInstance.catLower(lteq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("<= 1", result.toString());
		
		result = new StringAppender();
		testInstance.catLower(not(lteq(1)), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("> 1", result.toString());
	}
	
	@Test
	public void catEquals() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(eq(1), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("= 1", result.toString());
	}
	
	@Test
	public void catEquals_column() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), new StringAppenderWrapper(result, dmlNameProvider), null);
		assertEquals("= Toto.a", result.toString());
	}
	
	@Test
	public void catSum() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catSum(sum(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("sum(Toto.a)", result.toString());
	}
	
	@Test
	public void catCount() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catCount(count(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("count(Toto.a)", result.toString());
	}
	
	@Test
	public void catMin() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMin(min(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("min(Toto.a)", result.toString());
	}
	
	@Test
	public void catMax() {
		OperatorBuilder testInstance = new OperatorBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMax(max(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("max(Toto.a)", result.toString());
	}
}