package io.github.msvhora.tape

import com.squareup.burst.BurstJUnit4
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.util.UUID

/**
 * Created by Murtuza Vhora(@msvhora) on 03,April,2024
 */

@RunWith(BurstJUnit4::class)
class ConcurrentStressTest {
    @JvmField
    @Rule
    var folder = TemporaryFolder()

    private var queue: ObjectQueue<JsonData?>? = null

    @Before
    @Throws(IOException::class)
    fun setUp() = runTest {
        createQueueFile()
    }

    private fun createQueueFile() {
        val parent = folder.getRoot()
        val file = File(parent, "stress-object-queue")
        val queueFile: QueueFile = QueueFile.Builder(file).build()
        queue = ObjectQueue.create(queueFile, JsonConverter())
    }

    @Operation
    suspend fun add(n: Int) {
        createQueueFile()
        queue?.add(JsonData(n.toString()))
    }

    @Operation
    suspend fun size(): Int? {
        createQueueFile()
        return queue?.size()
    }

    @Operation
    suspend fun remove(n: Int) {
        createQueueFile()
        val size = size() ?: 0
        queue?.remove(minOf(size, n))
    }

    @Operation
    suspend fun peek(n: Int) {
        createQueueFile()
        val size = size() ?: 0
        queue?.peek(minOf(size, n))
    }

    @Operation
    suspend fun clear() {
        createQueueFile()
        queue?.clear()
    }

    @Test
    fun stressTest() = StressOptions()
        .threads(2)
        .actorsPerThread(2)
        .invocationsPerIteration(50000)
        .check(this::class)

    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .threads(2)
        .actorsPerThread(2)
        .invocationsPerIteration(50000)
        .check(this::class)

    @Serializable
    data class JsonData(
        @SerialName("value")
        val value: String,
        @SerialName("id")
        val id: String = UUID.randomUUID().toString(),
        @SerialName("list")
        val list: List<String> = listOf(
            "one",
            "two",
            "three",
            "four",
            "five",
        ),
        val intData: Int = -1,
        val longData: Long = 0L,
        val floatData: Float = 0f,
        val doubleData: Double = 0.0,
    ) {
        override fun equals(other: Any?): Boolean {
            return other is JsonData
                    && other.value == this.value
                    && other.list == list
                    && other.intData == intData
                    && other.longData == longData
                    && other.floatData == floatData
                    && other.doubleData == doubleData
        }

        override fun hashCode(): Int {
            var result = value.hashCode()
            result = 31 * result + id.hashCode()
            return result
        }
    }

    internal class JsonConverter : ObjectQueue.Converter<JsonData?> {
        private val json = Json

        @Throws(IOException::class)
        override fun from(source: ByteArray?): JsonData? {
            return source?.let {
                try {
                    json
                        .decodeFromString(
                            deserializer = JsonData.serializer(),
                            string = it.decodeToString()
                        )
                } catch (e: Exception) {
                    null
                }
            }
        }

        override fun toStream(value: JsonData?, sink: OutputStream?) {
            val jsonString = value?.let {
                json.encodeToString(
                    serializer = JsonData.serializer(),
                    value = it
                )
            }
            jsonString?.toByteArray(charset("UTF-8"))?.let { sink?.write(it) }
        }
    }
}