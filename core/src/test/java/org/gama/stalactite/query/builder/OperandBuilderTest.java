package org.gama.stalactite.query.builder;

import java.util.Collections;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.Operand;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.junit.Test;

import static org.gama.stalactite.query.model.Operand.between;
import static org.gama.stalactite.query.model.Operand.contains;
import static org.gama.stalactite.query.model.Operand.count;
import static org.gama.stalactite.query.model.Operand.endsWith;
import static org.gama.stalactite.query.model.Operand.eq;
import static org.gama.stalactite.query.model.Operand.gt;
import static org.gama.stalactite.query.model.Operand.gteq;
import static org.gama.stalactite.query.model.Operand.in;
import static org.gama.stalactite.query.model.Operand.lt;
import static org.gama.stalactite.query.model.Operand.lteq;
import static org.gama.stalactite.query.model.Operand.max;
import static org.gama.stalactite.query.model.Operand.min;
import static org.gama.stalactite.query.model.Operand.not;
import static org.gama.stalactite.query.model.Operand.startsWith;
import static org.gama.stalactite.query.model.Operand.sum;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class OperandBuilderTest {
	
	private DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap());
	
	@Test
	public void cat_nullValue_isTransformedToIsNull() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.cat(new Operand(null) {}, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is null", result.toString());
	}
	
	@Test
	public void catNullValue() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catNullValue(false, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is null", result.toString());
		
		result = new StringAppender();
		testInstance.catNullValue(true, new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("is not null", result.toString());
	}
	
	@Test
	public void catIsNull() {
		OperandBuilder testInstance = new OperandBuilder();
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
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLike(new Like("a"), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("like 'a'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(contains("a"), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("like '%a%'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(startsWith("a"), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("like 'a%'", result.toString());
		
		result = new StringAppender();
		testInstance.catLike(endsWith("a"), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("like '%a'", result.toString());	}
	
	@Test
	public void catIn() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catIn(in("a", "b"), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("in ('a', 'b')", result.toString());
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.catIn(in(), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("in ()", result.toString());
		
		// next test is meant to record the behavior, not to approve it
		result = new StringAppender();
		testInstance.cat(in((Object) null), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("in (null)", result.toString());
	}
	
	@Test
	public void catBetween() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catBetween(between(1, 2), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("between 1 and 2", result.toString());
		
		result = new StringAppender();
		testInstance.catBetween(between(1, null), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("> 1", result.toString());
		
		result = new StringAppender();
		testInstance.catBetween(between(null, 2), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("< 2", result.toString());
	}
	
	@Test
	public void catGreater() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catGreater(gt(1), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("> 1", result.toString());
		
		result = new StringAppender();
		testInstance.catGreater(gteq(1), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals(">= 1", result.toString());
	}
	
	@Test
	public void catLower() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catLower(lt(1), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("< 1", result.toString());
		
		result = new StringAppender();
		testInstance.catLower(lteq(1), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("<= 1", result.toString());
	}
	
	@Test
	public void catEquals() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		testInstance.catEquals(eq(1), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("= 1", result.toString());
	}
	
	@Test
	public void catEquals_column() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catEquals(eq(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("= Toto.a", result.toString());
	}
	
	@Test
	public void catSum() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catSum(sum(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("sum(Toto.a)", result.toString());
	}
	
	@Test
	public void catCount() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catCount(count(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("count(Toto.a)", result.toString());
	}
	
	@Test
	public void catMin() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMin(min(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("min(Toto.a)", result.toString());
	}
	
	@Test
	public void catMax() {
		OperandBuilder testInstance = new OperandBuilder();
		StringAppender result = new StringAppender();
		
		Table tableToto = new Table("Toto");
		Column colA = tableToto.addColumn("a", Integer.class);
		
		testInstance.catMax(max(colA), new StringAppenderWrapper(result, dmlNameProvider));
		assertEquals("max(Toto.a)", result.toString());
	}
}