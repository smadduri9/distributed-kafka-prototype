package com.kafkads.protocol;

/**
 * Which side of a connection is decoding. Some api keys (e.g. PING) are shared by
 * request and response; the side disambiguates which concrete type to build.
 */
public enum Side {
    SERVER,
    CLIENT
}
