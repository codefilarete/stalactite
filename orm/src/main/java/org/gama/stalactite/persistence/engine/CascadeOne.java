package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeOne<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> {
	
	private final Function<SRC, O> targetProvider;
	private final Method member;
	private final Persister<O, J, ? extends Table> persister;
	private Column reverseSide;
	private boolean nullable = true;
	private RelationshipMode relationshipMode = RelationshipMode.READ_ONLY;
	
	CascadeOne(Function<SRC, O> targetProvider, Persister<O, J, ? extends Table> persister, Method method) {
		this.targetProvider = targetProvider;
		this.persister = persister;
		// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird thing).
		this.member = method;
	}
	
	/** Original method reference given for mapping */
	public Function<SRC, O> getTargetProvider() {
		return targetProvider;
	}
	
	/** Equivalent of {@link #targetProvider} as a Reflection API element */
	public Method getMember() {
		return member;
	}
	
	/** The {@link Persister} that will be used to persist the target of the relation */
	public Persister<O, J, ?> getPersister() {
		return persister;
	}
	
	/** Nullable option, mainly for column join and DDL schema generation */
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public Column getReverseSide() {
		return reverseSide;
	}
	
	public void setReverseSide(Column reverseSide) {
		this.reverseSide = reverseSide;
	}
	
	public RelationshipMode getRelationshipMode() {
		return relationshipMode;
	}
	
	public void setRelationshipMode(RelationshipMode relationshipMode) {
		this.relationshipMode = relationshipMode;
	}
}
