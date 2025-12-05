package org.codefilarete.stalactite.engine.model.survey;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.tool.collection.Arrays;

public class Answer implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String comment;
	
	private Set<Choice> choices;
	
	private Set<Choice> secondaryChoices;
	
	public Answer() {
	}
	
	public Answer(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Answer(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	public Set<Choice> getChoices() {
		return choices;
	}
	
	public void addChoices(Collection<Choice> choices) {
		this.choices.addAll(choices);
	}
	
	public void addChoices(Choice... choices) {
		if (this.choices == null) {
			this.choices = new LinkedHashSet<>();
		}
		this.choices.addAll(Arrays.asList(choices));
	}
	
	public void setChoices(Set<Choice> choices) {
		this.choices = choices;
	}
	
	public Set<Choice> getSecondaryChoices() {
		return secondaryChoices;
	}
	
	public void addSecondaryChoices(Collection<Choice> choices) {
		this.secondaryChoices.addAll(choices);
	}
	
	public void addSecondaryChoices(Choice... choices) {
		if (this.secondaryChoices == null) {
			this.secondaryChoices = new LinkedHashSet<>();
		}
		this.secondaryChoices.addAll(Arrays.asList(choices));
	}
	
	public void setSecondaryChoices(Set<Choice> secondaryChoices) {
		this.secondaryChoices = secondaryChoices;
	}
	
	@Override
	public String toString() {
		return "Answer{" +
				"id=" + id +
				'}';
	}
}
