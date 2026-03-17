package com.learnkafka.entity;

import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

        @Id
        @GeneratedValue
        private Integer libraryEventId;

        @Enumerated(EnumType.STRING)
        private EventType libraryEventType;

        @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
        @ToString.Exclude
        private Book bookId;
}
