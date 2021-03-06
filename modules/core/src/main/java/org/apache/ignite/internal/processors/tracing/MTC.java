/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings.NOOP_UNCLOSED_SURROUNDINGS;

/**
 * Mapped tracing context.
 *
 * Thread local context which holding the information for tracing.
 */
public class MTC {
    /***/
    private static final Span NOOP_SPAN = NoopSpan.INSTANCE;

    /** Thread local span holder. */
    private static ThreadLocal<Span> span = ThreadLocal.withInitial(() -> NOOP_SPAN);

    /**
     * @return Span which corresponded to current thread or null if it doesn't not set.
     */
    @NotNull public static Span span() {
        return span.get();
    }

    /**
     * Attach given span to current thread if it isn't null or attach empty span if it is null. Detach given span, close
     * it and return previous span when {@link TraceSurroundings#close()} would be called.
     *
     * @param startSpan Span which should be added to current thread.
     * @return {@link TraceSurroundings} for manage span life cycle.
     */
    public static TraceSurroundings support(Span startSpan) {
        Span oldSpan = span();

        if (startSpan != null && startSpan != NOOP_SPAN) {
            span.set(startSpan);

            return new TraceSurroundings(oldSpan, true);
        }
        else
            return oldSpan == NOOP_SPAN ? NOOP_UNCLOSED_SURROUNDINGS : new TraceSurroundings(oldSpan, false);
    }

    /**
     * Support initial span.
     *
     * @param startSpan Span which should be added to current thread.
     */
    public static void supportInitial(Span startSpan) {
        span.set(startSpan);
    }

    /**
     * Attach given span to current thread if it isn't null or attach empty span if it is null.
     *
     * @param startSpan Span which should be added to current thread.
     * @return {@link TraceSurroundings} for manage span life cycle.
     */
    public static TraceSurroundings supportContinual(Span startSpan) {
        Span oldSpan = span();

        if (startSpan != null && startSpan != NOOP_SPAN)
            span.set(startSpan);

        return new TraceSurroundings(oldSpan, false);
    }

    /**
     * Helper for managing of span life cycle. It help to end current span and also reattach previous one to thread after
     * {@link TraceSurroundings#close()} would be call.
     */
    public static class TraceSurroundings implements AutoCloseable {
        /** Span which should be attached to thread when {@code #close} would be called. */
        private final Span oldSpan;

        /** {@code true} if current span should be ended at close moment. */
        private final boolean endRequired;

        /** Precreated instance of current class for performance target. */
        static final TraceSurroundings NOOP_UNCLOSED_SURROUNDINGS = new TraceSurroundings(NOOP_SPAN, false);

        /**
         * @param oldSpan Old span for restoring after close.
         * @param endRequired {@code true} if current span should be ended at close moment.
         */
        private TraceSurroundings(Span oldSpan, boolean endRequired) {
            this.oldSpan = oldSpan;
            this.endRequired = endRequired;
        }

        /**
         * Close life cycle of current span.
         */
        @Override public void close() {
            if (endRequired)
                span.get().end();

            span.set(oldSpan);
        }
    }
}
