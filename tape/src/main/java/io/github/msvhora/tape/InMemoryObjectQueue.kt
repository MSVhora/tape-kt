// Copyright 2012 Square, Inc.
// Copyright 2024,
@file:Suppress("KDocUnresolvedReference")

package io.github.msvhora.tape

import java.util.ArrayDeque
import java.util.Collections
import java.util.Deque

internal class InMemoryObjectQueue<T> : ObjectQueue<T> {
    private val entries: Deque<T>

    /**
     * The number of times this file has been structurally modified — it is incremented during [ ][.remove] and [.add]. Used by [InMemoryObjectQueue.EntryIterator] to
     * guard against concurrent modification.
     */
    var modCount = 0

    var closed = false

    init {
        entries = ArrayDeque()
    }

    override fun file(): QueueFile? {
        return null
    }

    @Throws(IllegalStateException::class)
    override suspend fun add(entry: T) {
        check(!closed) { "closed" }
        modCount++
        entries.addLast(entry)
    }

    @Throws(IllegalStateException::class)
    override suspend fun peek(): T? {
        check(!closed) { "closed" }
        return entries.peekFirst()
    }

    override suspend fun asList(): List<T> {
        return Collections.unmodifiableList(ArrayList(entries))
    }

    override suspend fun size(): Int {
        return entries.size
    }

    @Throws(IllegalStateException::class)
    override suspend fun remove(n: Int) {
        check(!closed) { "closed" }
        modCount++
        for (i in 0 until n) {
            entries.removeFirst()
        }
    }

    /**
     * Returns an iterator over entries in this queue.
     *
     *
     * The iterator disallows modifications to the queue during iteration. Removing entries from
     * the head of the queue is permitted during iteration using[Iterator.remove].
     */
    override operator fun iterator(): ObjectQueue.Iterator<T> {
        return EntryIterator(entries.iterator())
    }

    override fun close() {
        closed = true
    }

    override fun toString(): String {
        return ("InMemoryObjectQueue{"
                + "size=" + entries.size
                + '}')
    }

    private inner class EntryIterator(private val delegate: Iterator<T>) :
        ObjectQueue.Iterator<T> {
        private var index = 0

        /**
         * The [.modCount] value that the iterator believes that the backing QueueFile should
         * have. If this expectation is violated, the iterator has detected concurrent modification.
         */
        private var expectedModCount = modCount

        @Throws(ConcurrentModificationException::class)
        override operator fun hasNext(): Boolean {
            checkForComodification()
            return delegate.hasNext()
        }

        @Throws(
            ConcurrentModificationException::class,
            IllegalStateException::class
        )
        override operator fun next(): T {
            check(!closed) { "closed" }
            checkForComodification()
            val next = delegate.next()
            index += 1
            return next
        }

        @Throws(
            ConcurrentModificationException::class,
            IllegalStateException::class,
            NoSuchElementException::class,
            UnsupportedOperationException::class
        )
        override suspend fun remove() {
            check(!closed) { "closed" }
            checkForComodification()
            if (size() == 0) throw NoSuchElementException()
            if (index != 1) {
                throw UnsupportedOperationException("Removal is only permitted from the head.")
            }
            this@InMemoryObjectQueue.remove()
            expectedModCount = modCount
            index -= 1
        }

        private fun checkForComodification() {
            if (modCount != expectedModCount) throw ConcurrentModificationException()
        }
    }
}
