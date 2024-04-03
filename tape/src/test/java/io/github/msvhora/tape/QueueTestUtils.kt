// Copyright 2012 Square, Inc.
package io.github.msvhora.tape

import okio.buffer
import okio.sink
import okio.source
import org.junit.Assert
import java.io.File
import java.io.IOException

internal class QueueTestUtils private constructor() {
    init {
        throw AssertionError("No instances.")
    }

    /**
     * File that suppresses deletion.
     */
    internal class UnDeletableFile(name: String) : File(name) {
        override fun delete(): Boolean {
            return false
        }

        companion object {
            private const val SERIAL_VERSION_UID = 1L
        }
    }

    companion object {
        const val TRUNCATED_ONE_ENTRY_SERIALIZED_QUEUE = "/truncated-one-entry-serialized-queue"
        const val TRUNCATED_EMPTY_SERIALIZED_QUEUE = "/truncated-empty-serialized-queue"
        const val ONE_ENTRY_SERIALIZED_QUEUE = "/one-entry-serialized-queue"
        const val EMPTY_SERIALIZED_QUEUE = "/empty-serialized-queue"
        const val FRESH_SERIALIZED_QUEUE = "/fresh-serialized-queue"

        @Throws(IOException::class)
        fun copyTestFile(file: String): File {
            val newFile = File.createTempFile(file, "test")
            val stream = QueueTestUtils::class.java.getResourceAsStream(file)
            if (stream != null) {
                newFile.sink().buffer().use { sink -> sink.writeAll(stream.source()) }
            }
            Assert.assertTrue(newFile.exists())
            return newFile
        }
    }
}
