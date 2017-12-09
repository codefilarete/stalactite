package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class GroupBy implements Iterable<Object /* String, Column */>, GroupByChain {
	
	/** Column, String */
	private final List<Object> groups = new ArrayList<>();
	
	public GroupBy() {
	}
	
	private GroupBy add(Object table) {
		this.groups.add(table);
		return this;
	}
	
	public List<Object> getGroups() {
		return groups;
	}
	
	public GroupBy add(Column column, Column... columns) {
		add(column);
		for (Column col : columns) {
			add(col);
		}
		return this;
	}
	
	public GroupBy add(String column, String... columns) {
		add(column);
		for (String col : columns) {
			add(col);
		}
		return this;
	}
	
	@Override
	public Iterator<Object> iterator() {
		return this.groups.iterator();
	}
	
}
