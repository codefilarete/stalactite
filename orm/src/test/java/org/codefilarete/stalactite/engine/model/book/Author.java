package org.codefilarete.stalactite.engine.model.book;

import java.util.HashSet;
import java.util.Set;

public class Author extends AbstractEntity {

    private String name;

    private Set<Book> writtenBooks;
	
	public Author() {
	}
	
	public Author(String name) {
		this.name = name;
	}
    
    public String getName() {
        return name;
    }
	
	public Set<Book> getBooks() {
		if (writtenBooks == null) {
			writtenBooks = new HashSet<>();
		}
		return writtenBooks;
	}
	
	public void setBooks(Set<Book> books) {
		this.writtenBooks = books;
	}
	
	public void addBook(Book book) {
		getWrittenBooks().add(book);
	}
	
	public Set<Book> getWrittenBooks() {
        return writtenBooks;
    }
	
	public void setWrittenBooks(Set<Book> writtenBooks) {
		this.writtenBooks = writtenBooks;
	}
}
