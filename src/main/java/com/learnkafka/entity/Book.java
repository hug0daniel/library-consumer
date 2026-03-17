package com.learnkafka.entity;


import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

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
}
