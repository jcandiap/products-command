package com.jcandia.course.springcloud.kafka.command.models;

public record Reply<T>(
        String status,
        String message,
        T body) {
}
