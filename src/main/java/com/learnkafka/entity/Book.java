package com.learnkafka.entity;


import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Objects;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
        @Id
        @GeneratedValue
        private Integer bookId;

        private String bookName;
        private String bookAuthor;

        @OneToOne
        @JoinColumn(name ="libraryEventId")
        private LibraryEvent libraryEvent;

        @Override
        public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) return false;
                Book book = (Book) o;
                return Objects.equals(bookId, book.bookId) && Objects.equals(bookName, book.bookName) && Objects.equals(bookAuthor, book.bookAuthor) && Objects.equals(libraryEvent, book.libraryEvent);
        }

        @Override
        public int hashCode() {
                return Objects.hash(bookId, bookName, bookAuthor, libraryEvent);
        }
}
