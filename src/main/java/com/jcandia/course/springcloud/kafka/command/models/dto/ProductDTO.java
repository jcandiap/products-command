package com.jcandia.course.springcloud.kafka.command.models.dto;

public record ProductDTO(
        Long id,
        String name,
        Double price
) {
}
