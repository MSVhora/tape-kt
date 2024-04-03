// Copyright 2010 Square, Inc.
package io.github.msvhora.tape

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.test.runTest
import okio.BufferedSource
import okio.buffer
import okio.source
import org.junit.Assert
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.ArrayDeque
import java.util.Queue
import java.util.logging.Logger

/**
 * Tests for QueueFile.
 *
 * @author Bob Lee (bob@squareup.com)
 */
@RunWith(Parameterized::class)
class QueueFileTest(
    name: String,
    private val forceLegacy: Boolean,
    private val headerLength: Int,
) {
    @JvmField
    @Rule
    var folder = TemporaryFolder()
    private var file: File? = null

    @Throws(IOException::class)
    private fun newQueueFile(): QueueFile {
        return newQueueFile(true)
    }

    @Throws(IOException::class)
    private fun newQueueFile(raf: RandomAccessFile): QueueFile {
        return QueueFile(file!!, raf, true, forceLegacy)
    }

    @Throws(IOException::class)
    private fun newQueueFile(zero: Boolean): QueueFile {
        return QueueFile.Builder(file).zero(zero).forceLegacy(forceLegacy).build()
    }

    @Before
    @Throws(Exception::class)
    fun setUp() {
        val parent = folder.getRoot()
        file = File(parent, "queue-file")
    }

    @Test
    @Throws(IOException::class)
    fun testAddOneElement() = runTest {
        // This test ensures that we update 'first' correctly.
        var queue: QueueFile = newQueueFile()
        val expected = values[253]
        queue.add(expected)
        assertThat(queue.peek()).isEqualTo(expected)
        queue.close()
        queue = newQueueFile()
        assertThat(queue.peek()).isEqualTo(expected)
    }

    @Test
    @Throws(IOException::class)
    fun testClearErases() = runTest {
        val queue: QueueFile = newQueueFile()
        val expected = values[253]
        queue.add(expected)

        // Confirm that the data was in the file before we cleared.
        val data = ByteArray(expected!!.size)
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, expected.size)
        assertThat(data).isEqualTo(expected)
        queue.clear()

        // Should have been erased.
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, expected.size)
        assertThat(data).isEqualTo(ByteArray(expected.size))
    }

    @Test
    @Throws(IOException::class)
    fun testClearDoesNotCorrupt() = runTest {
        var queue: QueueFile = newQueueFile()
        val stuff = values[253]
        queue.add(stuff)
        queue.clear()
        queue = newQueueFile()
        assertThat(queue.isEmpty).isTrue()
        assertThat(queue.peek()).isNull()
        queue.add(values[25])
        assertThat(queue.peek()).isEqualTo(values[25])
    }

    @Test
    @Throws(IOException::class)
    fun removeErasesEagerly() = runTest {
        val queue: QueueFile = newQueueFile()
        val firstStuff = values[127]
        queue.add(firstStuff)
        val secondStuff = values[253]
        queue.add(secondStuff)

        // Confirm that first stuff was in the file before we remove.
        val data = ByteArray(firstStuff!!.size)
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, firstStuff.size)
        assertThat(data).isEqualTo(firstStuff)
        queue.remove()

        // Next record is intact
        assertThat(queue.peek()).isEqualTo(secondStuff)

        // First should have been erased.
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, firstStuff.size)
        assertThat(data).isEqualTo(ByteArray(firstStuff.size))
    }

    @Test
    @Throws(IOException::class)
    fun testZeroSizeInHeaderThrows() = runTest {
        val emptyFile = RandomAccessFile(file, "rwd")
        emptyFile.setLength(QueueFile.INITIAL_LENGTH.toLong())
        emptyFile.channel.force(true)
        emptyFile.close()
        try {
            newQueueFile()
            Assert.fail("Should have thrown about bad header length")
        } catch (ex: IOException) {
            assertThat(ex).hasMessageThat()
                .isEqualTo("File is corrupt; length stored in header (0) is invalid.")
        }
    }

    @Test
    @Throws(IOException::class)
    fun testSizeLessThanHeaderThrows() {
        val emptyFile = RandomAccessFile(file, "rwd")
        emptyFile.setLength(QueueFile.INITIAL_LENGTH.toLong())
        if (forceLegacy) {
            emptyFile.writeInt(headerLength - 1)
        } else {
            emptyFile.writeInt(-0x7fffffff)
            emptyFile.writeLong((headerLength - 1).toLong())
        }
        emptyFile.channel.force(true)
        emptyFile.close()
        try {
            newQueueFile()
            Assert.fail()
        } catch (ex: IOException) {
            assertThat(ex.message).isIn(
                mutableListOf(
                    "File is corrupt; length stored in header (15) is invalid.",
                    "File is corrupt; length stored in header (31) is invalid."
                )
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun testNegativeSizeInHeaderThrows() {
        val emptyFile = RandomAccessFile(file, "rwd")
        emptyFile.seek(0)
        emptyFile.writeInt(-2147483648)
        emptyFile.setLength(QueueFile.INITIAL_LENGTH.toLong())
        emptyFile.channel.force(true)
        emptyFile.close()
        try {
            newQueueFile()
            Assert.fail("Should have thrown about bad header length")
        } catch (ex: IOException) {
            assertThat(ex.message).isIn(
                mutableListOf(
                    "File is corrupt; length stored in header (-2147483648) is invalid.",
                    "Unable to read version 0 format. Supported versions are 1 and legacy."
                )
            )
        }
    }

    @Test
    @Throws(IOException::class)
    fun removeMultipleDoesNotCorrupt() = runTest {
        var queue: QueueFile = newQueueFile()
        for (i in 0..9) {
            queue.add(values[i])
        }
        queue.remove(1)
        assertThat(queue.size()).isEqualTo(9)
        assertThat(queue.peek()).isEqualTo(values[1])
        queue.remove(3)
        queue = newQueueFile()
        assertThat(queue.size()).isEqualTo(6)
        assertThat(queue.peek()).isEqualTo(values[4])
        queue.remove(6)
        assertThat(queue.isEmpty).isTrue()
        assertThat(queue.peek()).isNull()
    }

    @Test
    @Throws(IOException::class)
    fun removeDoesNotCorrupt() = runTest {
        var queue: QueueFile = newQueueFile()
        queue.add(values[127])
        val secondStuff = values[253]
        queue.add(secondStuff)
        queue.remove()
        queue = newQueueFile()
        assertThat(queue.peek()).isEqualTo(secondStuff)
    }

    @Test
    @Throws(IOException::class)
    fun removeFromEmptyFileThrows() = runTest {
        val queue: QueueFile = newQueueFile()
        try {
            queue.remove()
            Assert.fail("Should have thrown about removing from empty file.")
        } catch (ignored: NoSuchElementException) {
        }
    }

    @Test
    @Throws(IOException::class)
    fun removeZeroFromEmptyFileDoesNothing() = runTest {
        val queue: QueueFile = newQueueFile()
        queue.remove(0)
        assertThat(queue.isEmpty).isTrue()
    }

    @Test
    @Throws(IOException::class)
    fun removeNegativeNumberOfElementsThrows() = runTest {
        val queue: QueueFile = newQueueFile()
        queue.add(values[127])
        try {
            queue.remove(-1)
            Assert.fail("Should have thrown about removing negative number of elements.")
        } catch (ex: IllegalArgumentException) {
            assertThat(ex) //
                .hasMessageThat().isEqualTo("Cannot remove negative (-1) number of elements.")
        }
    }

    @Test
    @Throws(IOException::class)
    fun removeZeroElementsDoesNothing() = runTest {
        val queue: QueueFile = newQueueFile()
        queue.add(values[127])
        queue.remove(0)
        assertThat(queue.size()).isEqualTo(1)
    }

    @Test
    @Throws(IOException::class)
    fun removeBeyondQueueSizeElementsThrows() = runTest {
        val queue: QueueFile = newQueueFile()
        queue.add(values[127])
        try {
            queue.remove(10)
            Assert.fail("Should have thrown about removing too many elements.")
        } catch (ex: IllegalArgumentException) {
            assertThat(ex) //
                .hasMessageThat()
                .isEqualTo("Cannot remove more elements (10) than present in queue (1).")
        }
    }

    @Test
    @Throws(IOException::class)
    fun removingBigDamnBlocksErasesEffectively() = runTest {
        val bigBoy = ByteArray(7000)
        var i = 0
        while (i < 7000) {
            System.arraycopy(values[100]!!, 0, bigBoy, i, values[100]!!.size)
            i += 100
        }
        val queue: QueueFile = newQueueFile()
        queue.add(bigBoy)
        val secondStuff = values[123]
        queue.add(secondStuff)

        // Confirm that bigBoy was in the file before we remove.
        val data = ByteArray(bigBoy.size)
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, bigBoy.size)
        assertThat(data).isEqualTo(bigBoy)
        queue.remove()

        // Next record is intact
        assertThat(queue.peek()).isEqualTo(secondStuff)

        // First should have been erased.
        queue.raf.seek(headerLength + QueueFile.Element.HEADER_LENGTH.toLong())
        queue.raf.readFully(data, 0, bigBoy.size)
        assertThat(data).isEqualTo(ByteArray(bigBoy.size))
    }

    @Test
    @Throws(IOException::class)
    fun testAddAndRemoveElements() = runTest {
        val start = System.nanoTime()
        val expected: Queue<ByteArray?> = ArrayDeque()
        for (round in 0..4) {
            val queue: QueueFile = newQueueFile()
            for (i in 0 until N) {
                queue.add(values[i])
                expected.add(values[i])
            }

            // Leave N elements in round N, 15 total for 5 rounds. Removing all the
            // elements would be like starting with an empty queue.
            for (i in 0 until N - round - 1) {
                assertThat(queue.peek()).isEqualTo(expected.remove())
                queue.remove()
            }
            queue.close()
        }

        // Remove and validate remaining 15 elements.
        val queue: QueueFile = newQueueFile()
        assertThat(queue.size()).isEqualTo(15)
        assertThat(queue.size()).isEqualTo(expected.size)
        while (!expected.isEmpty()) {
            assertThat(queue.peek()).isEqualTo(expected.remove())
            queue.remove()
        }
        queue.close()

        // length() returns 0, but I checked the size w/ 'ls', and it is correct.
        // assertEquals(65536, file.length());
        logger.info("Ran in " + (System.nanoTime() - start) / 1000000 + "ms.")
    }

    /**
     * Tests queue expansion when the data crosses EOF.
     */
    @Test
    @Throws(IOException::class)
    fun testSplitExpansion() = runTest {
        // This should result in 3560 bytes.
        val max = 80
        val expected: Queue<ByteArray?> = ArrayDeque()
        val queue: QueueFile = newQueueFile()
        for (i in 0 until max) {
            expected.add(values[i])
            queue.add(values[i])
        }

        // Remove all but 1.
        for (i in 1 until max) {
            assertThat(queue.peek()).isEqualTo(expected.remove())
            queue.remove()
        }

        // This should wrap around before expanding.
        for (i in 0 until N) {
            expected.add(values[i])
            queue.add(values[i])
        }
        while (!expected.isEmpty()) {
            assertThat(queue.peek()).isEqualTo(expected.remove())
            queue.remove()
        }
        queue.close()
    }

    /**
     * Tests failed queue expansion when the data crosses EOF.
     */
    @Test
    @Throws(IOException::class)
    fun testFailedSplitExpansion() = runTest {
        // This should results in a full file, but doesn't trigger an expansion (yet)
        val max = 8
        val expected: Queue<ByteArray?> = ArrayDeque()
        var queue: QueueFile = newQueueFile()
        for (i in 0 until max) {
            expected.add(values[i])
            queue.add(values[i])
        }

        // Remove all but 1 value and add back
        // This should wrap around before expanding.
        for (i in 0 until max - 1) {
            assertThat(queue.peek()).isEqualTo(expected.remove())
            queue.remove()
            expected.add(values[i])
            queue.add(values[i])
        }

        //Try to insert element that causes file expansion, but fail
        val fileLengthBeforeExpansion = file!!.length()
        val braf = BrokenRandomAccessFile(file, "rwd")
        queue = newQueueFile(braf)
        try {
            queue.add(values[max])
            Assert.fail()
        } catch (e: IOException) { /* expected */
        }

        //Check that the queue continues valid
        braf.rejectCommit = false
        while (!expected.isEmpty()) {
            assertThat(queue.peek()).isEqualTo(expected.remove())
            queue.remove()
        }
        queue.close()
    }

    @Test
    @Throws(IOException::class)
    fun testFailedAdd() = runTest {
        var queueFile: QueueFile = newQueueFile()
        queueFile.add(values[253])
        queueFile.close()
        val braf = BrokenRandomAccessFile(file, "rwd")
        queueFile = newQueueFile(braf)
        try {
            queueFile.add(values[252])
            Assert.fail()
        } catch (e: IOException) { /* expected */
        }
        braf.rejectCommit = false

        // Allow a subsequent add to succeed.
        queueFile.add(values[251])
        queueFile.close()
        queueFile = newQueueFile()
        assertThat(queueFile.size()).isEqualTo(2)
        assertThat(queueFile.peek()).isEqualTo(values[253])
        queueFile.remove()
        assertThat(queueFile.peek()).isEqualTo(values[251])
    }

    @Test
    @Throws(IOException::class)
    fun testFailedRemoval() = runTest {
        var queueFile: QueueFile = newQueueFile()
        queueFile.add(values[253])
        queueFile.close()
        val braf = BrokenRandomAccessFile(file, "rwd")
        queueFile = newQueueFile(braf)
        try {
            queueFile.remove()
            Assert.fail()
        } catch (e: IOException) { /* expected */
        }
        queueFile.close()
        queueFile = newQueueFile()
        assertThat(queueFile.size()).isEqualTo(1)
        assertThat(queueFile.peek()).isEqualTo(values[253])
        queueFile.add(values[99])
        queueFile.remove()
        assertThat(queueFile.peek()).isEqualTo(values[99])
    }

    @Test
    @Throws(IOException::class)
    fun testFailedExpansion() = runTest {
        var queueFile: QueueFile = newQueueFile()
        queueFile.add(values[253])
        queueFile.close()
        val braf = BrokenRandomAccessFile(file, "rwd")
        queueFile = newQueueFile(braf)
        try {
            // This should trigger an expansion which should fail.
            queueFile.add(ByteArray(8000))
            Assert.fail()
        } catch (e: IOException) { /* expected */
        }
        queueFile.close()
        queueFile = newQueueFile()
        assertThat(queueFile.size()).isEqualTo(1)
        assertThat(queueFile.peek()).isEqualTo(values[253])
        assertThat(queueFile.fileLength).isEqualTo(4096)
        queueFile.add(values[99])
        queueFile.remove()
        assertThat(queueFile.peek()).isEqualTo(values[99])
    }

    /**
     * Exercise a bug where wrapped elements were getting corrupted when the
     * QueueFile was forced to expand in size and a portion of the final Element
     * had been wrapped into space at the beginning of the file.
     */
    @Test
    @Throws(IOException::class)
    fun testFileExpansionDoesntCorruptWrappedElements() = runTest {
        val queue: QueueFile = newQueueFile()

        // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
        val values = arrayOfNulls<ByteArray>(5)
        for (blockNum in values.indices) {
            values[blockNum] = ByteArray(1024)
            for (i in values[blockNum]!!.indices) {
                values[blockNum]!![i] = (blockNum + 1).toByte()
            }
        }

        // First, add the first two blocks to the queue, remove one leaving a
        // 1K space at the beginning of the buffer.
        queue.add(values[0])
        queue.add(values[1])
        queue.remove()

        // The trailing end of block "4" will be wrapped to the start of the buffer.
        queue.add(values[2])
        queue.add(values[3])

        // Cause buffer to expand as there isn't space between the end of block "4"
        // and the start of block "2".  Internally the queue should cause block "4"
        // to be contiguous, but there was a bug where that wasn't happening.
        queue.add(values[4])

        // Make sure values are not corrupted, specifically block "4" that wasn't
        // being made contiguous in the version with the bug.
        for (blockNum in 1 until values.size) {
            val value = queue.peek()
            queue.remove()
            value?.let {
                for (i in it.indices) {
                    assertThat(it[i]).isEqualTo((blockNum + 1).toByte())
                }
            }
        }
        queue.close()
    }

    /**
     * Exercise a bug where wrapped elements were getting corrupted when the
     * QueueFile was forced to expand in size and a portion of the final Element
     * had been wrapped into space at the beginning of the file - if multiple
     * Elements have been written to empty buffer space at the start does the
     * expansion correctly update all their positions?
     */
    @Test
    @Throws(IOException::class)
    fun testFileExpansionCorrectlyMovesElements() = runTest {
        val queue: QueueFile = newQueueFile()

        // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
        val values = arrayOfNulls<ByteArray>(5)
        for (blockNum in values.indices) {
            values[blockNum] = ByteArray(1024)
            for (i in values[blockNum]!!.indices) {
                values[blockNum]!![i] = (blockNum + 1).toByte()
            }
        }

        // smaller data elements
        val smaller = arrayOfNulls<ByteArray>(3)
        for (blockNum in smaller.indices) {
            smaller[blockNum] = ByteArray(256)
            for (i in smaller[blockNum]!!.indices) {
                smaller[blockNum]!![i] = (blockNum + 6).toByte()
            }
        }

        // First, add the first two blocks to the queue, remove one leaving a
        // 1K space at the beginning of the buffer.
        queue.add(values[0])
        queue.add(values[1])
        queue.remove()

        // The trailing end of block "4" will be wrapped to the start of the buffer.
        queue.add(values[2])
        queue.add(values[3])

        // Now fill in some space with smaller blocks, none of which will cause
        // an expansion.
        queue.add(smaller[0])
        queue.add(smaller[1])
        queue.add(smaller[2])

        // Cause buffer to expand as there isn't space between the end of the
        // smaller block "8" and the start of block "2".  Internally the queue
        // should cause all of tbe smaller blocks, and the trailing end of
        // block "5" to be moved to the end of the file.
        queue.add(values[4])
        val expectedBlockNumbers = byteArrayOf(2, 3, 4, 6, 7, 8, 5)

        // Make sure values are not corrupted, specifically block "4" that wasn't
        // being made contiguous in the version with the bug.
        for (expectedBlockNumber: Byte in expectedBlockNumbers) {
            val value = queue.peek()
            queue.remove()
            value?.let {
                for (i in it.indices) {
                    assertThat(it[i]).isEqualTo(expectedBlockNumber)
                }

            }
        }
        queue.close()
    }

    @Test
    @Throws(IOException::class)
    fun removingElementZeroesData() = runTest {
        val queueFile: QueueFile = newQueueFile(true)
        queueFile.add(values[4])
        queueFile.remove()
        queueFile.close()
        val source: BufferedSource? = file?.source()?.buffer()
        source?.skip(headerLength.toLong())
        source?.skip(QueueFile.Element.HEADER_LENGTH.toLong())
        assertThat(source?.readByteString(4)?.hex()).isEqualTo("00000000")
    }

    @Test
    @Throws(IOException::class)
    fun removingElementDoesNotZeroData() = runTest {
        val queueFile: QueueFile = newQueueFile(false)
        queueFile.add(values[4])
        queueFile.remove()
        queueFile.close()
        val source: BufferedSource? = file?.source()?.buffer()
        source?.skip(headerLength.toLong())
        source?.skip(QueueFile.Element.HEADER_LENGTH.toLong())
        assertThat(source?.readByteString(4)?.hex()).isEqualTo("04030201")
        source?.close()
    }

    /**
     * Exercise a bug where file expansion would leave garbage at the start of the header
     * and after the last element.
     */
    @Test
    @Throws(IOException::class)
    fun testFileExpansionCorrectlyZeroesData() = runTest {
        val queue: QueueFile = newQueueFile()

        // Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
        val values = arrayOfNulls<ByteArray>(5)
        for (blockNum in values.indices) {
            values[blockNum] = ByteArray(1024)
            for (i in values[blockNum]!!.indices) {
                values[blockNum]!![i] = (blockNum + 1).toByte()
            }
        }

        // First, add the first two blocks to the queue, remove one leaving a
        // 1K space at the beginning of the buffer.
        queue.add(values[0])
        queue.add(values[1])
        queue.remove()

        // The trailing end of block "4" will be wrapped to the start of the buffer.
        queue.add(values[2])
        queue.add(values[3])

        // Cause buffer to expand as there isn't space between the end of block "4"
        // and the start of block "2". Internally the queue will cause block "4"
        // to be contiguous. There was a bug where the start of the buffer still
        // contained the tail end of block "4", and garbage was copied after the tail
        // end of the last element.
        queue.add(values[4])

        // Read from header to first element and make sure it's zeroed.
        val firstElementPadding: Int = QueueFile.Element.HEADER_LENGTH + 1024
        var data: ByteArray = ByteArray(firstElementPadding)
        queue.raf.seek(headerLength.toLong())
        queue.raf.readFully(data, 0, firstElementPadding)
        assertThat(data).isEqualTo(ByteArray(firstElementPadding))

        // Read from the last element to the end and make sure it's zeroed.
        val endOfLastElement: Int =
            headerLength + firstElementPadding + 4 * (QueueFile.Element.HEADER_LENGTH + 1024)
        val readLength = (queue.raf.length() - endOfLastElement).toInt()
        data = ByteArray(readLength)
        queue.raf.seek(endOfLastElement.toLong())
        queue.raf.readFully(data, 0, readLength)
        assertThat(data).isEqualTo(ByteArray(readLength))
    }

    /**
     * Exercise a bug where an expanding queue file where the start and end positions
     * are the same causes corruption.
     */
    @Test
    @Throws(IOException::class)
    fun testSaturatedFileExpansionMovesElements() = runTest {
        val queue: QueueFile = newQueueFile()

        // Create test data - 1016-byte blocks marked consecutively 1, 2, 3, 4, 5 and 6,
        // four of which perfectly fill the queue file, taking into account the file header
        // and the item headers.
        // Each item is of length
        // (QueueFile.INITIAL_LENGTH - headerLength) / 4 - element_header_length
        // = 1016 bytes
        val values = arrayOfNulls<ByteArray>(6)
        for (blockNum in values.indices) {
            values[blockNum] =
                ByteArray((QueueFile.INITIAL_LENGTH - headerLength) / 4 - QueueFile.Element.HEADER_LENGTH)
            for (i in values[blockNum]!!.indices) {
                values[blockNum]!![i] = (blockNum + 1).toByte()
            }
        }

        // Saturate the queue file
        queue.add(values[0])
        queue.add(values[1])
        queue.add(values[2])
        queue.add(values[3])

        // Remove an element and add a new one so that the position of the start and
        // end of the queue are equal
        queue.remove()
        queue.add(values[4])

        // Cause the queue file to expand
        queue.add(values[5])

        // Make sure values are not corrupted
        for (i in 1..5) {
            assertThat(queue.peek()).isEqualTo(values[i])
            queue.remove()
        }
        queue.close()
    }

    /**
     * Exercise a bug where opening a queue whose first or last element's header
     * was non contiguous throws an [java.io.EOFException].
     */
    @Test
    @Throws(IOException::class)
    fun testReadHeadersFromNonContiguousQueueWorks() = runTest {
        val queueFile: QueueFile = newQueueFile()

        // Fill the queue up to `length - 2` (i.e. remainingBytes() == 2).
        for (i in 0..14) {
            queueFile.add(values[N - 1])
        }
        queueFile.add(values[219])

        // Remove first item so we have room to add another one without growing the file.
        queueFile.remove()

        // Add any element element and close the queue.
        queueFile.add(values[6])
        val queueSize: Int = queueFile.size()
        queueFile.close()

        // File should not be corrupted.
        val queueFile2: QueueFile = newQueueFile()
        assertThat(queueFile2.size()).isEqualTo(queueSize)
    }

    @Test
    @Throws(IOException::class)
    fun testIterator() = runTest {
        val data = values[10]
        for (i in 0..9) {
            val queueFile: QueueFile = newQueueFile()
            for (j in 0 until i) {
                queueFile.add(data)
            }
            var saw = 0
            for (element: ByteArray? in queueFile) {
                assertThat(element).isEqualTo(data)
                saw++
            }
            assertThat(saw).isEqualTo(i)
            queueFile.close()
            file!!.delete()
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorNextThrowsWhenEmpty() {
        val queueFile: QueueFile = newQueueFile()
        val iterator = queueFile.iterator()
        try {
            iterator.next()
            Assert.fail()
        } catch (ignored: NoSuchElementException) {
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorNextThrowsWhenExhausted() = runTest {
        val queueFile: QueueFile = newQueueFile()
        queueFile.add(values[0])
        val iterator = queueFile.iterator()
        iterator.next()
        try {
            iterator.next()
            Assert.fail()
        } catch (ignored: NoSuchElementException) {
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorRemove() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        val iterator = queueFile.iterator()
        while (iterator.hasNext()) {
            iterator.next()
            iterator.remove()
        }
        assertThat(queueFile.isEmpty).isTrue()
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorRemoveDisallowsConcurrentModification() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        val iterator = queueFile.iterator()
        iterator.next()
        queueFile.remove()
        try {
            iterator.remove()
        } catch (ignored: ConcurrentModificationException) {
            Assert.fail()
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorHasNextDisallowsConcurrentModification() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        val iterator = queueFile.iterator()
        iterator.next()
        queueFile.remove()
        try {
            iterator.hasNext()
        } catch (ignored: ConcurrentModificationException) {
            Assert.fail()
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorDisallowsConcurrentModificationWithClear() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        val iterator = queueFile.iterator()
        iterator.next()
        queueFile.clear()
        try {
            iterator.hasNext()
        } catch (ignored: ConcurrentModificationException) {
            Assert.fail()
        }
    }

    @Test
    @Throws(IOException::class)
    fun testIteratorOnlyRemovesFromHead() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        val iterator = queueFile.iterator()
        iterator.next()
        iterator.next()
        try {
            iterator.remove()
            Assert.fail()
        } catch (ex: UnsupportedOperationException) {
            assertThat(ex).hasMessageThat().isEqualTo("Removal is only permitted from the head.")
        }
    }

    @Test
    @Throws(IOException::class)
    fun iteratorThrowsIOException() = runTest {
        var queueFile: QueueFile = newQueueFile()
        queueFile.add(values[253])
        queueFile.close()
        class BrokenRandomAccessFile(file: File?, mode: String?) :
            RandomAccessFile(file, mode) {
            var fail = false

            @Throws(IOException::class)
            override fun write(b: ByteArray, off: Int, len: Int) {
                if (fail) {
                    throw IOException()
                }
                super.write(b, off, len)
            }

            @Throws(IOException::class)
            override fun read(b: ByteArray, off: Int, len: Int): Int {
                if (fail) {
                    throw IOException()
                }
                return super.read(b, off, len)
            }
        }

        val braf = BrokenRandomAccessFile(file, "rwd")
        queueFile = newQueueFile(braf)
        val iterator = queueFile.iterator()
        braf.fail = true
        try {
            iterator.next()
            Assert.fail()
        } catch (ioe: Exception) {
            assertThat(ioe).isInstanceOf(IOException::class.java)
        }
        braf.fail = false
        iterator.next()
        braf.fail = true
        try {
            iterator.remove()
            Assert.fail()
        } catch (ioe: Exception) {
            assertThat(ioe).isInstanceOf(IOException::class.java)
        }
    }

    @Test
    @Throws(IOException::class)
    fun queueToString() = runTest {
        val queueFile: QueueFile = newQueueFile()
        for (i in 0..14) {
            queueFile.add(values[i])
        }
        if (forceLegacy) {
            assertThat(queueFile.toString()).contains(
                "zero=true, versioned=false, length=4096,"
                        + " size=15,"
                        + " first=Element[position=16, length=0], last=Element[position=163, length=14]}"
            )
        } else {
            assertThat(queueFile.toString()).contains(
                ("zero=true, versioned=true, length=4096,"
                        + " size=15,"
                        + " first=Element[position=32, length=0], last=Element[position=179, length=14]}")
            )
        }
    }

    /**
     * A RandomAccessFile that can break when you go to write the COMMITTED
     * status.
     */
    internal class BrokenRandomAccessFile(file: File?, mode: String?) :
        RandomAccessFile(file, mode) {
        var rejectCommit = true

        @Throws(IOException::class)
        override fun write(b: ByteArray, off: Int, len: Int) {
            if (rejectCommit && filePointer == 0L) {
                throw IOException("No commit for you!")
            }
            super.write(b, off, len)
        }
    }

    companion object {
        private val logger = Logger.getLogger(QueueFileTest::class.java.name)

        /**
         * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of 255 so that the number
         * of bytes isn't a multiple of 4.
         */
        private const val N = 254
        private val values = arrayOfNulls<ByteArray>(N)

        init {
            for (i in 0 until N) {
                val value = ByteArray(i)
                // Example: values[3] = { 3, 2, 1 }
                for (ii in 0 until i) value[ii] = (i - ii).toByte()
                values[i] = value
            }
        }

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun parameters(): List<Array<Any>> {
            return listOf(arrayOf("Legacy", true, 16), arrayOf("Versioned", false, 32))
        }
    }
}
