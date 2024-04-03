/*
* Copyright (C) 2010 Square, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
@file:Suppress("KDocUnresolvedReference", "SameParameterValue")

package io.github.msvhora.tape

import io.github.msvhora.tape.QueueFile.Builder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.Closeable
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import kotlin.math.min

/**
 * A reliable, efficient, file-based, FIFO queue. Additions and removals are O(1). All operations
 * are atomic. Writes are synchronous; data will be written to disk before an operation returns.
 * The underlying file is structured to survive process and even system crashes. If an I/O
 * exception is thrown during a mutating change, the change is aborted. It is safe to continue to
 * use a `QueueFile` instance after an exception.
 *
 *
 * **Note that this implementation is not synchronized.**
 *
 *
 * In a traditional queue, the remove operation returns an element. In this queue,
 * [.peek] and [.remove] are used in conjunction. Use
 * `peek` to retrieve the first element, and then `remove` to remove it after
 * successful processing. If the system crashes after `peek` and during processing, the
 * element will remain in the queue, to be processed when the system restarts.
 *
 *
 * **NOTE:** The current implementation is built for file systems that support
 * atomic segment writes (like YAFFS). Most conventional file systems don't support this; if the
 * power goes out while writing a segment, the segment will contain garbage and the file will be
 * corrupt. We'll add journaling support so this class can be used with more file systems later.
 *
 *
 * Construct instances with [Builder].
 *
 * @author Bob Lee (bob@squareup.com)
 */
class QueueFile internal constructor(
    file: File,
    raf: RandomAccessFile,
    zero: Boolean,
    forceLegacy: Boolean,
) :
    Closeable {
    /**
     * The underlying file. Uses a ring buffer to store entries. Designed so that a modification
     * isn't committed or visible until we write the header. The header is much smaller than a
     * segment. So long as the underlying file system supports atomic segment writes, changes to the
     * queue are atomic. Storing the file length ensures we can recover from a failed expansion
     * (i.e. if setting the file length succeeds but the process dies before the data can be copied).
     *
     *
     * This implementation supports two versions of the on-disk format.
     * <pre>
     * Format:
     * 16-32 bytes      Header
     * ...              Data
     *
     * Header (32 bytes):
     * 1 bit            Versioned indicator [0 = legacy (see "Legacy Header"), 1 = versioned]
     * 31 bits          Version, always 1
     * 8 bytes          File length
     * 4 bytes          Element count
     * 8 bytes          Head element position
     * 8 bytes          Tail element position
     *
     * Legacy Header (16 bytes):
     * 1 bit            Legacy indicator, always 0 (see "Header")
     * 31 bits          File length
     * 4 bytes          Element count
     * 4 bytes          Head element position
     * 4 bytes          Tail element position
     *
     * Element:
     * 4 bytes          Data length
     * ...              Data
    </pre> *
     */
    val raf: RandomAccessFile

    /**
     * Keep file around for error reporting.
     */
    private val file: File

    /**
     * True when using the versioned header format. Otherwise use the legacy format.
     */
    private val versioned: Boolean

    /**
     * The header length in bytes: 16 or 32.
     */
    private var headerLength = 0

    /**
     * In-memory buffer. Big enough to hold the header.
     */
    private val buffer = ByteArray(32)

    /**
     * When true, removing an element will also overwrite data with zero bytes.
     */
    private val zero: Boolean

    /**
     * Cached file length. Always a power of 2.
     */
    var fileLength: Long = 0

    /**
     * Number of elements.
     */
    private var elementCount = 0

    /**
     * Pointer to first (or eldest) element.
     */
    private var first: Element

    /**
     * The number of times this file has been structurally modified — it is incremented during
     * [.remove] and [.add]. Used by [ElementIterator]
     * to guard against concurrent modification.
     */
    private var modCount = 0

    private var closed = false

    /**
     * Pointer to last (or newest) element.
     */
    private var last: Element

    /**
     * Mutex for waiting functionality
     */
    private val mutex = Mutex()

    init {
        this.file = file
        this.raf = raf
        this.zero = zero
        raf.seek(0)
        raf.readFully(buffer)
        versioned = !forceLegacy && buffer[0].toInt() and 0x80 != 0
        val firstOffset: Long
        val lastOffset: Long
        if (versioned) {
            headerLength = 32
            val version = readInt(buffer, 0) and 0x7FFFFFFF
            if (version != 1) {
                throw IOException(
                    "Unable to read version $version format. Supported versions are 1 and legacy."
                )
            }
            fileLength = readLong(buffer, 4)
            elementCount = readInt(buffer, 12)
            firstOffset = readLong(buffer, 16)
            lastOffset = readLong(buffer, 24)
        } else {
            headerLength = 16
            fileLength = readInt(buffer, 0).toLong()
            elementCount = readInt(buffer, 4)
            firstOffset = readInt(buffer, 8).toLong()
            lastOffset = readInt(buffer, 12).toLong()
        }
        if (fileLength > raf.length()) {
            throw IOException(
                "File is truncated. Expected length: " + fileLength + ", Actual length: " + raf.length()
            )
        } else if (fileLength <= headerLength) {
            throw IOException(
                "File is corrupt; length stored in header ($fileLength) is invalid."
            )
        }
        first = readElement(firstOffset)
        last = readElement(lastOffset)
    }

    /**
     * Writes header atomically. The arguments contain the updated values. The class member fields
     * should not have changed yet. This only updates the state in the file. It's up to the caller to
     * update the class member variables *after* this call succeeds. Assumes segment writes are
     * atomic in the underlying file system.
     */
    @Throws(IOException::class)
    private fun writeHeader(
        fileLength: Long,
        elementCount: Int,
        firstPosition: Long,
        lastPosition: Long,
    ) {
        raf.seek(0)
        if (versioned) {
            writeInt(buffer, 0, VERSIONED_HEADER)
            writeLong(buffer, 4, fileLength)
            writeInt(buffer, 12, elementCount)
            writeLong(buffer, 16, firstPosition)
            writeLong(buffer, 24, lastPosition)
            raf.write(buffer, 0, 32)
            return
        }

        // Legacy queue header.
        writeInt(buffer, 0, fileLength.toInt()) // Signed, so leading bit is always 0 aka legacy.
        writeInt(buffer, 4, elementCount)
        writeInt(buffer, 8, firstPosition.toInt())
        writeInt(buffer, 12, lastPosition.toInt())
        raf.write(buffer, 0, 16)
    }

    @Throws(IOException::class)
    private fun readElement(position: Long): Element {
        if (position == 0L) return Element.NULL
        ringRead(position, buffer, 0, Element.HEADER_LENGTH)
        val length = readInt(buffer, 0)
        return Element(position, length)
    }

    /**
     * Wraps the position if it exceeds the end of the file.
     */
    private fun wrapPosition(position: Long): Long {
        return if (position < fileLength) position else headerLength + position - fileLength
    }

    /**
     * Writes count bytes from buffer to position in file. Automatically wraps write if position is
     * past the end of the file or if buffer overlaps it.
     *
     * @param position in file to write to
     * @param buffer   to write from
     * @param count    # of bytes to write
     */
    @Throws(IOException::class)
    private fun ringWrite(position: Long, buffer: ByteArray, offset: Int, count: Int) {
        var curPosition = position
        curPosition = wrapPosition(curPosition)
        if (curPosition + count <= fileLength) {
            raf.seek(curPosition)
            raf.write(buffer, offset, count)
        } else {
            // The write overlaps the EOF.
            // # of bytes to write before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
            val beforeEof = (fileLength - curPosition).toInt()
            raf.seek(curPosition)
            raf.write(buffer, offset, beforeEof)
            raf.seek(headerLength.toLong())
            raf.write(buffer, offset + beforeEof, count - beforeEof)
        }
    }

    @Throws(IOException::class)
    private fun ringErase(position: Long, length: Long) {
        var curPosition = position
        var curLength = length
        while (curLength > 0) {
            val chunk = min(curLength, ZEROES.size.toLong()).toInt()
            ringWrite(curPosition, ZEROES, 0, chunk)
            curLength -= chunk.toLong()
            curPosition += chunk.toLong()
        }
    }

    /**
     * Reads count bytes into buffer from file. Wraps if necessary.
     *
     * @param position in file to read from
     * @param buffer   to read into
     * @param count    # of bytes to read
     */
    @Throws(IOException::class)
    private fun ringRead(position: Long, buffer: ByteArray, offset: Int, count: Int) {
        var curPosition = position
        curPosition = wrapPosition(curPosition)
        if (curPosition + count <= fileLength) {
            raf.seek(curPosition)
            raf.readFully(buffer, offset, count)
        } else {
            // The read overlaps the EOF.
            // # of bytes to read before the EOF. Guaranteed to be less than Integer.MAX_VALUE.
            val beforeEof = (fileLength - curPosition).toInt()
            raf.seek(curPosition)
            raf.readFully(buffer, offset, beforeEof)
            raf.seek(headerLength.toLong())
            raf.readFully(buffer, offset + beforeEof, count - beforeEof)
        }
    }

    /**
     * Adds an element to the end of the queue.
     *
     * @param data   to copy bytes from
     * @param offset to start from in buffer
     * @param count  number of bytes to copy
     * @throws IndexOutOfBoundsException if `offset < 0` or `count < 0`, or if `offset + count` is bigger than the length of `buffer`.
     */
    @JvmOverloads
    @Throws(IOException::class)
    suspend fun add(data: ByteArray?, offset: Int = 0, count: Int = data?.size ?: 0) {
        mutex.withLock {
            if (data == null) {
                throw NullPointerException("data == null")
            }
            if (offset or count < 0 || count > data.size - offset) {
                throw IndexOutOfBoundsException()
            }
            check(!closed) { "closed" }
            expandIfNecessary(count.toLong())

            // Insert a new element after the current last element.
            val wasEmpty = isEmpty
            val position =
                if (wasEmpty) headerLength.toLong() else wrapPosition(last.position + Element.HEADER_LENGTH + last.length)
            val newLast =
                Element(position, count)

            // Write length.
            writeInt(buffer, 0, count)
            ringWrite(
                newLast.position,
                buffer,
                0,
                Element.HEADER_LENGTH
            )

            // Write data.
            ringWrite(
                newLast.position + Element.HEADER_LENGTH,
                data,
                offset,
                count
            )

            // Commit the addition. If wasEmpty, first == last.
            val firstPosition: Long = if (wasEmpty) newLast.position else first.position
            writeHeader(fileLength, elementCount + 1, firstPosition, newLast.position)
            last = newLast
            elementCount++
            modCount++
            if (wasEmpty) first = last // first element
        }
    }

    private fun usedBytes(): Long {
        if (elementCount == 0) return headerLength.toLong()
        return if (last.position >= first.position) {
            // Contiguous queue.
            ((last.position - first.position) // all but last entry
                    + Element.HEADER_LENGTH + last.length // last entry
                    + headerLength)
        } else {
            // tail < head. The queue wraps.
            (last.position // buffer front + header
                    + Element.HEADER_LENGTH + last.length // last entry
                    + fileLength) - first.position // buffer end
        }
    }

    private fun remainingBytes(): Long {
        return fileLength - usedBytes()
    }

    val isEmpty: Boolean
        /**
         * Returns true if this queue contains no entries.
         */
        get() = elementCount == 0

    /**
     * If necessary, expands the file to accommodate an additional element of the given length.
     *
     * @param dataLength length of data being added
     */
    @Throws(IOException::class)
    private fun expandIfNecessary(dataLength: Long) {
        val elementLength: Long =
            Element.HEADER_LENGTH + dataLength
        var remainingBytes = remainingBytes()
        if (remainingBytes >= elementLength) return

        // Expand.
        var previousLength = fileLength
        var newLength: Long
        // Double the length until we can fit the new data.
        do {
            remainingBytes += previousLength
            newLength = previousLength shl 1
            previousLength = newLength
        } while (remainingBytes < elementLength)
        setLength(newLength)

        // Calculate the position of the tail end of the data in the ring buffer
        val endOfLastElement =
            wrapPosition(last.position + Element.HEADER_LENGTH + last.length)
        var count: Long = 0
        // If the buffer is split, we need to make it contiguous
        if (endOfLastElement <= first.position) {
            val channel = raf.channel
            channel.position(fileLength) // destination position
            count = endOfLastElement - headerLength
            if (channel.transferTo(headerLength.toLong(), count, channel) != count) {
                throw AssertionError("Copied insufficient number of bytes!")
            }
        }

        // Commit the expansion.
        if (last.position < first.position) {
            val newLastPosition: Long = fileLength + last.position - headerLength
            writeHeader(newLength, elementCount, first.position, newLastPosition)
            last = Element(newLastPosition, last.length)
        } else {
            writeHeader(newLength, elementCount, first.position, last.position)
        }
        fileLength = newLength
        if (zero) {
            ringErase(headerLength.toLong(), count)
        }
    }

    /**
     * Sets the length of the file.
     */
    @Throws(IOException::class)
    private fun setLength(newLength: Long) {
        // Set new file length (considered metadata) and sync it to storage.
        raf.setLength(newLength)
        raf.channel.force(true)
    }

    /**
     * Reads the eldest element. Returns null if the queue is empty.
     */
    @Throws(IOException::class)
    suspend fun peek(): ByteArray? {
        mutex.withLock {
            check(!closed) { "closed" }
            if (isEmpty) return null
            val length: Int = first.length
            val data = ByteArray(length)
            ringRead(
                first.position + Element.HEADER_LENGTH,
                data,
                0,
                length
            )
            return data
        }
    }

    /**
     * Returns an iterator over elements in this QueueFile.
     *
     *
     * The iterator disallows modifications to be made to the QueueFile during iteration. Removing
     * elements from the head of the QueueFile is permitted during iteration using
     * [Iterator.remove].
     *
     *
     * The iterator may throw an unchecked [IOException] during [Iterator.next]
     * or [Iterator.remove].
     */
    operator fun iterator(): ObjectQueue.Iterator<ByteArray?> {
        return ElementIterator()
    }

    /**
     * Returns the number of elements in this queue.
     */
    suspend fun size(): Int {
        mutex.withLock {
            return elementCount
        }
    }

    /**
     * Removes the eldest `n` elements.
     *
     * @throws NoSuchElementException if the queue is empty
     */
    /**
     * Removes the eldest element.
     *
     * @throws NoSuchElementException if the queue is empty
     */
    @JvmOverloads
    @Throws(
        IOException::class,
        IllegalArgumentException::class,
        IllegalStateException::class,
        NoSuchElementException::class
    )
    suspend fun remove(n: Int = 1) {
        mutex.lock()
        require(n >= 0) { "Cannot remove negative ($n) number of elements." }
        if (n == 0) {
            mutex.unlock()
            return
        }
        if (n == elementCount) {
            mutex.unlock()
            clear()
            return
        }
        if (isEmpty) {
            mutex.unlock()
            throw NoSuchElementException()
        }
        if (n > elementCount) {
            mutex.unlock()
            throw IllegalArgumentException(
                "Cannot remove more elements ($n) than present in queue ($elementCount)."
            )
        }
        mutex.unlock()

        mutex.withLock {
            val eraseStartPosition: Long = first.position
            var eraseTotalLength: Long = 0

            // Read the position and length of the new first element.
            var newFirstPosition: Long = first.position
            var newFirstLength: Int = first.length
            for (i in 0 until n) {
                eraseTotalLength += (Element.HEADER_LENGTH + newFirstLength).toLong()
                newFirstPosition =
                    wrapPosition(newFirstPosition + Element.HEADER_LENGTH + newFirstLength)
                ringRead(
                    newFirstPosition,
                    buffer,
                    0,
                    Element.HEADER_LENGTH
                )
                newFirstLength = readInt(buffer, 0)
            }

            // Commit the header.
            writeHeader(fileLength, elementCount - n, newFirstPosition, last.position)
            elementCount -= n
            modCount++
            first = Element(newFirstPosition, newFirstLength)
            if (zero) {
                ringErase(eraseStartPosition, eraseTotalLength)
            }
        }
    }

    /**
     * Clears this queue. Truncates the file to the initial size.
     */
    @Throws(
        IOException::class,
        IllegalStateException::class,
    )
    suspend fun clear() {
        mutex.withLock {
            if (closed) throw IllegalStateException("closed")

            // Commit the header.
            writeHeader(INITIAL_LENGTH.toLong(), 0, 0, 0)
            if (zero) {
                // Zero out data.
                raf.seek(headerLength.toLong())
                raf.write(ZEROES, 0, INITIAL_LENGTH - headerLength)
            }
            elementCount = 0
            first = Element.NULL
            last = Element.NULL
            if (fileLength > INITIAL_LENGTH) setLength(INITIAL_LENGTH.toLong())
            fileLength = INITIAL_LENGTH.toLong()
            modCount++
        }
    }

    /**
     * The underlying [File] backing this queue.
     */
    fun file(): File {
        return file
    }

    @Throws(IOException::class)
    override fun close() {
        closed = true
        raf.close()
    }

    override fun toString(): String {
        return ("QueueFile{"
                + "file=" + file
                + ", zero=" + zero
                + ", versioned=" + versioned
                + ", length=" + fileLength
                + ", size=" + elementCount
                + ", first=" + first
                + ", last=" + last
                + '}')
    }

    /**
     * Constructs a new element.
     *
     * @param position within file
     * @param length   of data
     */
    class Element(
        val position: Long,
        val length: Int,
    ) {
        override fun toString(): String {
            return (javaClass.simpleName
                    + "[position=" + position
                    + ", length=" + length
                    + "]")
        }

        companion object {
            val NULL: Element =
                Element(0, 0)

            /**
             * Length of element header in bytes.
             */
            const val HEADER_LENGTH = 4
        }
    }

    /**
     * Fluent API for creating [QueueFile] instances.
     */
    class Builder(file: File?) {
        private val file: File
        private var zero = true
        private var forceLegacy = false

        /**
         * Start constructing a new queue backed by the given file.
         */
        init {
            if (file == null) {
                throw NullPointerException("file == null")
            }
            this.file = file
        }

        /**
         * When true, removing an element will also overwrite data with zero bytes.
         */
        fun zero(zero: Boolean): Builder {
            this.zero = zero
            return this
        }

        /**
         * When true, only the legacy (Tape 1.x) format will be used.
         */
        fun forceLegacy(forceLegacy: Boolean): Builder {
            this.forceLegacy = forceLegacy
            return this
        }

        /**
         * Constructs a new queue backed by the given builder. Only one instance should access a given
         * file at a time.
         */
        @Throws(IOException::class)
        fun build(): QueueFile {
            val raf = initializeFromFile(file, forceLegacy)
            var qf: QueueFile? = null
            return try {
                qf = QueueFile(file, raf, zero, forceLegacy)
                qf
            } finally {
                if (qf == null) {
                    raf.close()
                }
            }
        }
    }

    private inner class ElementIterator() :
        ObjectQueue.Iterator<ByteArray?> {
        /**
         * Index of element to be returned by subsequent call to next.
         */
        var nextElementIndex = 0

        /**
         * Position of element to be returned by subsequent call to next.
         */
        private var nextElementPosition: Long = first.position

        override operator fun hasNext(): Boolean {
            if (closed) throw IllegalStateException("closed")
            return nextElementIndex != elementCount
        }

        @Throws(IOException::class)
        override operator fun next(): ByteArray {
            if (closed) throw IllegalStateException("closed")
            if (isEmpty) throw NoSuchElementException()
            if (nextElementIndex >= elementCount) throw NoSuchElementException()
            return try {
                // Read the current element.
                val current: Element = readElement(nextElementPosition)
                val buffer = ByteArray(current.length)
                nextElementPosition =
                    wrapPosition(current.position + Element.HEADER_LENGTH)
                ringRead(nextElementPosition, buffer, 0, current.length)

                // Update the pointer to the next element.
                nextElementPosition =
                    wrapPosition(current.position + Element.HEADER_LENGTH + current.length)
                nextElementIndex++

                // Return the read element.
                buffer
            } catch (e: IOException) {
                throw e
            }
        }

        @Throws(IOException::class)
        override suspend fun remove() {
            if (isEmpty) throw NoSuchElementException()
            if (nextElementIndex != 1) {
                throw UnsupportedOperationException("Removal is only permitted from the head.")
            }
            try {
                this@QueueFile.remove()
            } catch (e: IOException) {
                throw e
            }
            nextElementIndex--
        }
    }

    companion object {
        /**
         * Initial file size in bytes.
         */
        const val INITIAL_LENGTH = 4096 // one file system block

        /**
         * Leading bit set to 1 indicating a versioned header and the version of 1.
         */
        private const val VERSIONED_HEADER = -0x7fffffff

        /**
         * A block of nothing to write over old data.
         */
        private val ZEROES = ByteArray(INITIAL_LENGTH)

        @Throws(IOException::class)
        fun initializeFromFile(file: File, forceLegacy: Boolean): RandomAccessFile {
            if (!file.exists()) {
                // Use a temp file so we don't leave a partially-initialized file.
                val tempFile = File(file.path + ".tmp")
                val readOnlyFile = open(tempFile)
                readOnlyFile.use { raf ->
                    raf.setLength(INITIAL_LENGTH.toLong())
                    raf.seek(0)
                    if (forceLegacy) {
                        raf.writeInt(INITIAL_LENGTH)
                    } else {
                        raf.writeInt(VERSIONED_HEADER)
                        raf.writeLong(INITIAL_LENGTH.toLong())
                    }
                }

                // A rename is atomic.
                if (!tempFile.renameTo(file)) {
                    throw IOException("Rename failed!")
                }
            }
            return open(file)
        }

        /**
         * Opens a random access file that writes synchronously.
         */
        @Throws(FileNotFoundException::class)
        private fun open(file: File): RandomAccessFile {
            return RandomAccessFile(file, "rwd")
        }

        /**
         * Stores an `int` in the `byte[]`. The behavior is equivalent to calling
         * [RandomAccessFile.writeInt].
         */
        private fun writeInt(buffer: ByteArray, offset: Int, value: Int) {
            buffer[offset] = (value shr 24).toByte()
            buffer[offset + 1] = (value shr 16).toByte()
            buffer[offset + 2] = (value shr 8).toByte()
            buffer[offset + 3] = value.toByte()
        }

        /**
         * Reads an `int` from the `byte[]`.
         */
        private fun readInt(buffer: ByteArray, offset: Int): Int {
            return ((buffer[offset].toInt() and 0xff shl 24)
                    + (buffer[offset + 1].toInt() and 0xff shl 16)
                    + (buffer[offset + 2].toInt() and 0xff shl 8)
                    + (buffer[offset + 3].toInt() and 0xff))
        }

        /**
         * Stores an `long` in the `byte[]`. The behavior is equivalent to calling
         * [RandomAccessFile.writeLong].
         */
        private fun writeLong(buffer: ByteArray, offset: Int, value: Long) {
            buffer[offset] = (value shr 56).toByte()
            buffer[offset + 1] = (value shr 48).toByte()
            buffer[offset + 2] = (value shr 40).toByte()
            buffer[offset + 3] = (value shr 32).toByte()
            buffer[offset + 4] = (value shr 24).toByte()
            buffer[offset + 5] = (value shr 16).toByte()
            buffer[offset + 6] = (value shr 8).toByte()
            buffer[offset + 7] = value.toByte()
        }

        /**
         * Reads an `long` from the `byte[]`.
         */
        private fun readLong(buffer: ByteArray, offset: Int): Long {
            return ((buffer[offset].toLong() and 0xffL shl 56)
                    + (buffer[offset + 1].toLong() and 0xffL shl 48)
                    + (buffer[offset + 2].toLong() and 0xffL shl 40)
                    + (buffer[offset + 3].toLong() and 0xffL shl 32)
                    + (buffer[offset + 4].toLong() and 0xffL shl 24)
                    + (buffer[offset + 5].toLong() and 0xffL shl 16)
                    + (buffer[offset + 6].toLong() and 0xffL shl 8)
                    + (buffer[offset + 7].toLong() and 0xffL))
        }
    }
}
