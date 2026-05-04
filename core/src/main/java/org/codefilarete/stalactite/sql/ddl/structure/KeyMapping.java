package org.codefilarete.stalactite.sql.ddl.structure;

import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.pair;

/**
 * 
 * @param <LEFTTABLE> the left table of the join
 * @param <RIGHTTABLE> the right table of the join
 * @param <ID> the type of the join key
 * @author Guillaume Mary
 */
public class KeyMapping<LEFTTABLE extends Fromable, RIGHTTABLE extends Fromable, ID> implements Key<LEFTTABLE, ID> {
	
	private final Key<LEFTTABLE, ID> leftKey;
	
	private final Key<RIGHTTABLE, ID> rightKey;
	
	private final KeepOrderMap<JoinLink<LEFTTABLE, ?>, JoinLink<RIGHTTABLE, ?>> mapping;
	
	
	public KeyMapping(Key<LEFTTABLE, ID> leftKey, Key<RIGHTTABLE, ID> rightKey) {
		this.leftKey = leftKey;
		this.rightKey = rightKey;
		this.mapping = pair(leftKey.getColumns(), rightKey.getColumns(), KeepOrderMap::new);
	}
	
	public Key<LEFTTABLE, ID> getLeftKey() {
		return leftKey;
	}
	
	public Key<RIGHTTABLE, ID> getRightKey() {
		return rightKey;
	}
	
	public <J1 extends JoinLink<LEFTTABLE, ?>, J2 extends JoinLink<RIGHTTABLE, ?>> KeepOrderMap<J1, J2> getMapping() {
		return (KeepOrderMap<J1, J2>) mapping;
	}
	
	public <J extends JoinLink<LEFTTABLE, ?>> KeepOrderSet<J> getLeftColumns() {
		return (KeepOrderSet<J>) new KeepOrderSet<>(mapping.keySet());
	}
	
	
	public <J extends JoinLink<RIGHTTABLE, ?>> KeepOrderSet<J> getRightColumns() {
		return (KeepOrderSet<J>) new KeepOrderSet<>(mapping.values());
	}
	
	@Override
	public LEFTTABLE getTable() {
		return leftKey.getTable();
	}
	
	@Override
	public <J extends JoinLink<LEFTTABLE, ?>> KeepOrderSet<J> getColumns() {
		return getLeftColumns();
	}
	
	@Override
	public boolean isComposed() {
		return leftKey.isComposed();
	}
}
