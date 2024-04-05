// Copyright 2011 Square, Inc.
// Copyright 2024, Murtuza Vhora<murtazavhora@gmail.com>
package com.github.msvhora.tape

import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.util.Collections
import kotlin.math.min

/**
 * This code is migrated from Java to Kotlin with its original logic intact.
 *
 * @author Murtuza Vhora (murtazavhora@gmail.com)
 */
/**
 * A queue of objects.
 */
interface ObjectQueue<T> : Closeable {
    /**
     * The underlying [QueueFile] backing this queue, or null if it's only in memory.
     */
    fun file(): QueueFile?

    /**
     * Returns the number of entries in the queue.
     */
    suspend fun size(): Int

    suspend fun isEmpty(): Boolean {
        /**
         * Returns `true` if this queue contains no entries.
         */
        return size() == 0
    }

    /**
     * Enqueues an entry that can be processed at any time.
     */
    @Throws(IOException::class)
    suspend fun add(entry: T)

    /**
     * Returns the head of the queue, or `null` if the queue is empty. Does not modify the
     * queue.
     */
    @Throws(IOException::class)
    suspend fun peek(): T?

    /**
     * Returns the iterator for the queue
     */
    operator fun iterator(): Iterator<T>


    /**
     * Reads up to `max` entries from the head of the queue without removing the entries.
     * If the queue's [.size] is less than `max` then only [.size] entries
     * are read.
     */
    @Throws(IOException::class)
    suspend fun peek(max: Int): List<T?> {
        val end = min(max, size())
        val subList: MutableList<T?> = ArrayList(end)
        val iterator: Iterator<T> = iterator()
        for (i in 0 until end) {
            subList.add(iterator.next())
        }
        return Collections.unmodifiableList(subList)
    }

    /**
     * Returns the entries in the queue as an unmodifiable [List].
     */
    @Throws(IOException::class)
    suspend fun asList(): List<T?> {
        return peek(size())
    }

    /**
     * Removes the head of the queue.
     */
    @Throws(IOException::class)
    suspend fun remove() {
        remove(1)
    }

    /**
     * Removes `n` entries from the head of the queue.
     */
    @Throws(IOException::class)
    suspend fun remove(n: Int)

    /**
     * Clears this queue. Also truncates the file to the initial size.
     */
    @Throws(IOException::class)
    suspend fun clear() {
        remove(size())
    }

    /**
     * Checks whether queue is closed or not
     */
    fun isClosed(): Boolean

    /**
     * Convert a byte stream to and from a concrete type.
     *
     * @param <T> Object type.
    </T> */
    interface Converter<T> {
        /**
         * Converts bytes to an object.
         */
        @Throws(IOException::class)
        fun from(source: ByteArray): T?

        /**
         * Converts `value` to bytes written to the specified stream.
         */
        @Throws(IOException::class)
        fun toStream(value: T, sink: OutputStream)
    }

    interface Iterator<T> {
        /**
         * Returns `true` if the iteration has more elements.
         */
        operator fun hasNext(): Boolean

        /**
         * Returns the next element in the iteration.
         *
         * @throws NoSuchElementException if the iteration has no next element.
         */
        @Throws(NoSuchElementException::class)
        operator fun next(): T

        /**
         * Removes from the underlying collection the last element returned by this iterator.
         *
         * @throws IllegalStateException if [next] has not been called yet,
         * or the most recent [next] call has already been followed by a [remove] call.
         */
        @Throws(IllegalStateException::class)
        suspend fun remove()
    }

    companion object {
        /**
         * A queue for objects that are atomically and durably serialized to `file`.
         */
        fun <T> create(qf: QueueFile, converter: Converter<T>): ObjectQueue<T> {
            return FileObjectQueue(qf, converter)
        }

        /**
         * A queue for objects that are not serious enough to be written to disk. Objects in this queue
         * are kept in memory and will not be serialized.
         */
        fun <T> createInMemory(): ObjectQueue<T> {
            return InMemoryObjectQueue()
        }
    }
}
