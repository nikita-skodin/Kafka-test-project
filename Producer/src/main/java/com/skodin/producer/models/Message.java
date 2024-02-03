package com.skodin.producer.models;

public record Message(String from, String to, String payload) {
}
