package com.learn.reactivestreams.helpers;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;

import static java.util.Arrays.asList;

public final class PublisherHelpers {
    /**
     * Creates a {@code Publisher<ByteBuffer>} from the ByteBuffers
     * @param byteBuffers the bytebuffers
     * @return a {@code Publisher<ByteBuffer>}
     */
    public static Publisher<ByteBuffer> toPublisher(final ByteBuffer... byteBuffers) {
        return Flux.fromIterable(asList(byteBuffers));
    }

    private PublisherHelpers() {
    }
}
