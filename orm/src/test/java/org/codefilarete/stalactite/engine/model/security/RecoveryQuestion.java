package org.codefilarete.stalactite.engine.model.security;

import org.codefilarete.stalactite.engine.model.book.AbstractEntity;
import org.codefilarete.stalactite.engine.model.survey.Answer;

public class RecoveryQuestion extends AbstractEntity {
	
	private Answer answer;
	
	public Answer getAnswer() {
		return answer;
	}
	
	public void setAnswer(Answer answer) {
		this.answer = answer;
	}
}
