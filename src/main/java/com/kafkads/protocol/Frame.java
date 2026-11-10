package com.kafkads.protocol;

/**
 * One message on the wire together with its correlation id. A response carries the id
 * of the request it answers, so responses may arrive in any order.
 */
public record Frame(int correlationId, Message message) {
}
