package org.codefilarete.stalactite.engine.model.survey;

import java.util.Set;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.tool.bean.Objects;

public class Choice implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String label;
	
	private Set<Answer> answers;
	
	public Choice() {
	}
	
	public Choice(long id) {
		this.id = new PersistableIdentifier<>(id);
	}
	
	public Choice(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getLabel() {
		return label;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public Set<Answer> getAnswers() {
		return answers;
	}
	
	public void setAnswers(Set<Answer> answers) {
		this.answers = answers;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Choice)) return false;
		Choice choice = (Choice) o;
		return Objects.equals(id, choice.id);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(id);
	}
	
	@Override
	public String toString() {
		return "Choice{id=" + id.getDelegate() + ", label='" + label + '\'' + '}';
	}
}
