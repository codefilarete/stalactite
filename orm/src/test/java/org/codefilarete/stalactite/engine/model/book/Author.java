package org.codefilarete.stalactite.engine.model.book;

import java.util.HashSet;
import java.util.Set;

public class Author {

    private Long id;

    private String name;

    private Set<Book> writtenBooks = new HashSet<>();
	
	public Author() {
	}
	
	public Author(String name) {
		this.name = name;
	}
	
	public Long getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public Set<Book> getWrittenBooks() {
        return writtenBooks;
    }
	
	public void setWrittenBooks(Set<Book> writtenBooks) {
		this.writtenBooks = writtenBooks;
	}
}
