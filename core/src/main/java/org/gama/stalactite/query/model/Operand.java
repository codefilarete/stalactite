package org.gama.stalactite.query.model;

import org.gama.stalactite.query.model.operand.Between;
import org.gama.stalactite.query.model.operand.Equals;
import org.gama.stalactite.query.model.operand.Greater;
import org.gama.stalactite.query.model.operand.In;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.gama.stalactite.query.model.operand.Lower;

/**
 * @author Guillaume Mary
 */
public abstract class Operand {
	
	public static Operand eq(Object value) {
		return new Equals(value);
	}
	
	public static Operand not(Operand operand) {
		operand.not = true;
		return operand;
	}
	
	public static Operand lt(Object value) {
		return new Lower(value);
	}
	
	public static Operand lteq(Object value) {
		return new Lower(value, true);
	}
	
	public static Operand gt(Object value) {
		return new Greater(value);
	}
	
	public static Operand gteq(Object value) {
		return new Greater(value, true);
	}
	
	public static Operand between(Object value1, Object value2) {
		return new Between(value1, value2);
	}
	
	public static Operand in(Iterable value) {
		return new In(value);
	}
	
	public static Operand in(Object ... value) {
		return new In(value);
	}
	
	public static Operand isNull() {
		return new IsNull();
	}
	
	public static Operand isNotNull() {
		return not(isNull());
	}
	
	public static Operand like(String value) {
		return new Like(value);
	}
	
	public static Operand contains(String value) {
		return new Like(value, true, true);
	}
	
	public static Operand startsWith(String value) {
		return new Like(value, false, true);
	}
	
	public static Operand endsWith(String value) {
		return new Like(value, true, false);
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
}
