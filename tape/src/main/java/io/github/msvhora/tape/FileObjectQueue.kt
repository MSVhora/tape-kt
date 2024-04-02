// Copyright 2012 Square, Inc.
@file:Suppress("KDocUnresolvedReference")

package io.github.msvhora.tape

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.io.IOException

internal class FileObjectQueue<T>(
    queueFile: QueueFile,
    converter: ObjectQueue.Converter<T>,
) :
    ObjectQueue<T> {
    val converter: ObjectQueue.Converter<T>

    /**
     * Backing storage implementation.
     */
    private val queueFile: QueueFile

    init {
        this.queueFile = queueFile
        this.converter = converter
    }


    override fun file(): QueueFile {
        return queueFile
    }

    override suspend fun size(): Int {
        return queueFile.size()
    }

    override suspend fun isEmpty(): Boolean {
        return queueFile.isEmpty
    }

    @Throws(IOException::class)
    override suspend fun add(entry: T) {
        val bytes = DirectByteArrayOutputStream()
        converter.toStream(entry, bytes)
        queueFile.add(bytes.array, 0, bytes.size())
        withContext(Dispatchers.IO) {
            bytes.close()
        }
    }

    @Throws(IOException::class)
    override suspend fun peek(): T? {
        val bytes: ByteArray = queueFile.peek() ?: return null
        return converter.from(bytes)
    }

    @Throws(IOException::class)
    override suspend fun remove() {
        queueFile.remove()
    }

    @Throws(IOException::class)
    override suspend fun remove(n: Int) {
        queueFile.remove(n)
    }

    @Throws(IOException::class)
    override suspend fun clear() {
        queueFile.clear()
    }

    @Throws(IOException::class)
    override fun close() {
        queueFile.close()
    }

    /**
     * Returns an iterator over entries in this queue.
     *
     *
     * The iterator disallows modifications to the queue during iteration. Removing entries from
     * the head of the queue is permitted during iteration using [Iterator.remove].
     *
     *
     * The iterator may throw an unchecked [IOException] during [Iterator.next]
     * or [Iterator.remove].
     */
    override operator fun iterator(): ObjectQueue.Iterator<T> {
        return QueueFileIterator(queueFile.iterator())
    }

    override fun toString(): String {
        return ("FileObjectQueue{"
                + "queueFile=" + queueFile
                + '}')
    }

    /**
     * Enables direct access to the internal array. Avoids unnecessary copying.
     */
    private class DirectByteArrayOutputStream : ByteArrayOutputStream() {
        val array: ByteArray
            /**
             * Gets a reference to the internal byte array.  The [.size] method indicates how many
             * bytes contain actual data added since the last [.reset] call.
             */
            get() = buf
    }

    private inner class QueueFileIterator(val iterator: ObjectQueue.Iterator<ByteArray?>) :
        ObjectQueue.Iterator<T> {
        override fun hasNext(): Boolean {
            return iterator.hasNext()
        }

        @Throws(IOException::class)
        override fun next(): T {
            val data = iterator.next()
            return try {
                converter.from(data)
            } catch (e: IOException) {
                throw e
            }
        }

        @Throws(IOException::class)
        override suspend fun remove() {
            this@FileObjectQueue.remove()
        }
    }
}
