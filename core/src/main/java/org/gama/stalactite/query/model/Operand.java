package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.operand.Between;
import org.gama.stalactite.query.model.operand.Count;
import org.gama.stalactite.query.model.operand.Equals;
import org.gama.stalactite.query.model.operand.Greater;
import org.gama.stalactite.query.model.operand.In;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.gama.stalactite.query.model.operand.Lower;
import org.gama.stalactite.query.model.operand.Max;
import org.gama.stalactite.query.model.operand.Min;
import org.gama.stalactite.query.model.operand.Sum;

/**
 * @author Guillaume Mary
 */
public abstract class Operand {
	
	public static Equals eq(Object value) {
		return new Equals(value);
	}
	
	public static <I extends Operand> I not(I operand) {
		operand.setNot();
		return operand;
	}
	
	public static Lower lt(Object value) {
		return new Lower(value);
	}
	
	public static Lower lteq(Object value) {
		return new Lower(value, true);
	}
	
	public static Greater gt(Object value) {
		return new Greater(value);
	}
	
	public static Greater gteq(Object value) {
		return new Greater(value, true);
	}
	
	public static Between between(Object value1, Object value2) {
		return new Between(value1, value2);
	}
	
	public static In in(Iterable value) {
		return new In(value);
	}
	
	public static In in(Object ... value) {
		return new In(value);
	}
	
	public static IsNull isNull() {
		return new IsNull();
	}
	
	public static IsNull isNotNull() {
		return not(isNull());
	}
	
	public static Like like(String value) {
		return new Like(value);
	}
	
	public static Like contains(String value) {
		return new Like(value, true, true);
	}
	
	public static Like startsWith(String value) {
		return new Like(value, false, true);
	}
	
	public static Like endsWith(String value) {
		return new Like(value, true, false);
	}
	
	public static Sum sum(Column column) {
		return new Sum(column);
	}
	
	public static Count count(Column column) {
		return new Count(column);
	}
	
	public static Min min(Column column) {
		return new Min(column);
	}
	
	public static Max max(Column column) {
		return new Max(column);
	}
	
	private Object value;
	
	private boolean not;
	
	public Operand(Object value) {
		this.value = value;
	}
	
	public Object getValue() {
		return value;
	}
	
	public void setValue(Object value) {
		this.value = value;
	}
	
	public boolean isNot() {
		return not;
	}
	
	public void setNot(boolean not) {
		this.not = not;
	}
	
	public void setNot() {
		setNot(true);
	}
	
	public void flipNot() {
		this.not = !this.not;
	}
}
