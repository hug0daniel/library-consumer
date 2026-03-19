package com.learnkafka.repository;

import com.learnkafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;


public interface LibraryEventRepository extends CrudRepository<LibraryEvent,Integer> {
}
