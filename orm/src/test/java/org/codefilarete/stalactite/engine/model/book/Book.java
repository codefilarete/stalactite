package org.codefilarete.stalactite.engine.model.book;

import java.util.HashSet;
import java.util.Set;


public class Book extends AbstractEntity {

    private String title;

    private Double price;

    private String isbn;

    private Set<Author> authors = new HashSet<>();
	
	public Book() {
	}
	
	public Book(String title, Double price, String isbn) {
		this.title = title;
		this.price = price;
		this.isbn = isbn;
	}
	
    public String getTitle() {
        return title;
    }
    
    public Double getPrice() {
        return price;
    }
    
    public String getIsbn() {
        return isbn;
    }
    
    public Set<Author> getAuthors() {
        return authors;
    }
	
	public void setAuthors(Set<Author> authors) {
		this.authors = authors;
	}
}
