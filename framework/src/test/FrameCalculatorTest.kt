package com.minare.core.frames.coordinator.services

import com.minare.application.config.FrameConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**@DisplayName("FrameCalculatorService")
class FrameCalculatorServiceTest {

    private lateinit var calculator: FrameCalculatorService

    private fun createCalculator(frameDurationMs: Long = 100): FrameCalculatorService {
        val config = object : FrameConfiguration() {
            override val frameDurationMs: Long = frameDurationMs
        }
        return FrameCalculatorService(config)
    }

    @BeforeEach
    fun setUp() {
        calculator = createCalculator(100)
    }

    @Nested
    @DisplayName("timestampToLogicalFrame")
    inner class TimestampToLogicalFrame {

        @Test
        fun `sessionStart=1000, timestamp=1000 returns frame 0`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1000,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(0)
        }

        @Test
        fun `sessionStart=1000, timestamp=1100 returns frame 1`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1100,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(1)
        }

        @Test
        fun `sessionStart=1000, timestamp=1550 returns frame 5`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1550,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(5)
        }

        @Test
        fun `timestamp exactly on frame boundary returns correct frame`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1300,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(3)
        }

        @Test
        fun `timestamp 1ms before frame boundary returns frame N`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1199,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(1)
        }

        @Test
        fun `timestamp 1ms after frame boundary returns frame N+1`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1201,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(2)
        }

        @Test
        fun `sessionStartTimestamp=0 returns frame 0`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 5000,
                sessionStartTimestamp = 0
            )
            assertThat(frame).isEqualTo(0)
        }

        @Test
        fun `timestamp equals sessionStart exactly returns frame 0`() {
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 5000,
                sessionStartTimestamp = 5000
            )
            assertThat(frame).isEqualTo(0)
        }

        @Test
        fun `very large time difference returns correct large frame number`() {
            val threeHoursMs = 3 * 60 * 60 * 1000L
            val frame = calculator.timestampToLogicalFrame(
                timestamp = 1000 + threeHoursMs,
                sessionStartTimestamp = 1000
            )
            val expectedFrames = threeHoursMs / 100
            assertThat(frame).isEqualTo(expectedFrames)
        }

        @Test
        fun `50ms frame duration calculates correctly`() {
            val calc = createCalculator(50)
            val frame = calc.timestampToLogicalFrame(
                timestamp = 1250,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(5)
        }

        @Test
        fun `500ms frame duration calculates correctly`() {
            val calc = createCalculator(500)
            val frame = calc.timestampToLogicalFrame(
                timestamp = 2500,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(3)
        }

        @Test
        fun `1000ms frame duration calculates correctly`() {
            val calc = createCalculator(1000)
            val frame = calc.timestampToLogicalFrame(
                timestamp = 6000,
                sessionStartTimestamp = 1000
            )
            assertThat(frame).isEqualTo(5)
        }

        @Nested
        @DisplayName("Negative Frames (Session Boundaries)")
        inner class NegativeFrames {

            @Test
            fun `timestamp 100ms before sessionStart returns frame -1`() {
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 900,
                    sessionStartTimestamp = 1000
                )
                assertThat(frame).isEqualTo(-1)
            }

            @Test
            fun `timestamp 500ms before sessionStart returns frame -5`() {
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 500,
                    sessionStartTimestamp = 1000
                )
                assertThat(frame).isEqualTo(-5)
            }

            @Test
            fun `timestamp 50ms before sessionStart returns frame 0 due to truncation toward zero`() {
                // Kotlin integer division truncates toward zero
                // -50 / 100 = 0, not -1
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 950,
                    sessionStartTimestamp = 1000
                )
                assertThat(frame).isEqualTo(0)
            }

            @Test
            fun `timestamp 99ms before sessionStart returns frame 0 due to truncation toward zero`() {
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 901,
                    sessionStartTimestamp = 1000
                )
                assertThat(frame).isEqualTo(0)
            }

            @Test
            fun `timestamp exactly 1 frame duration before returns frame -1`() {
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 900,
                    sessionStartTimestamp = 1000
                )
                assertThat(frame).isEqualTo(-1)
            }

            @Test
            fun `negative frame calculation matches documented formula`() {
                // (timestamp - sessionStart) / frameDuration
                // (9900 - 10000) / 100 = -100 / 100 = -1
                val frame = calculator.timestampToLogicalFrame(
                    timestamp = 9900,
                    sessionStartTimestamp = 10000
                )
                assertThat(frame).isEqualTo(-1)
            }
        }
    }

    @Nested
    @DisplayName("getCurrentLogicalFrame")
    inner class GetCurrentLogicalFrame {

        @Test
        fun `sessionStartNanos=0 returns -1`() {
            val frame = calculator.getCurrentLogicalFrame(0)
            assertThat(frame).isEqualTo(-1)
        }

        @Test
        fun `recently started session returns frame 0 or higher`() {
            val sessionStartNanos = System.nanoTime()
            val frame = calculator.getCurrentLogicalFrame(sessionStartNanos)
            assertThat(frame).isGreaterThanOrEqualTo(0)
        }

        @Test
        fun `session started 1 frame ago returns frame 1 or higher`() {
            val frameDurationNanos = 100 * 1_000_000L
            val sessionStartNanos = System.nanoTime() - frameDurationNanos
            val frame = calculator.getCurrentLogicalFrame(sessionStartNanos)
            assertThat(frame).isGreaterThanOrEqualTo(1)
        }
    }

    @Nested
    @DisplayName("nanosToLogicalFrame")
    inner class NanosToLogicalFrame {

        @Test
        fun `0 nanos returns frame 0`() {
            val frame = calculator.nanosToLogicalFrame(0)
            assertThat(frame).isEqualTo(0)
        }

        @Test
        fun `1 frame duration in nanos returns frame 1`() {
            val frameDurationNanos = 100 * 1_000_000L
            val frame = calculator.nanosToLogicalFrame(frameDurationNanos)
            assertThat(frame).isEqualTo(1)
        }

        @Test
        fun `1_5 frame durations in nanos returns frame 1`() {
            val frameDurationNanos = 100 * 1_000_000L
            val oneAndHalfFrames = frameDurationNanos + (frameDurationNanos / 2)
            val frame = calculator.nanosToLogicalFrame(oneAndHalfFrames)
            assertThat(frame).isEqualTo(1)
        }

        @Test
        fun `very large elapsed time returns correct frame number`() {
            val oneHourNanos = 60L * 60 * 1_000_000_000
            val frame = calculator.nanosToLogicalFrame(oneHourNanos)
            val expectedFrames = oneHourNanos / (100 * 1_000_000L)
            assertThat(frame).isEqualTo(expectedFrames)
        }

        @Test
        fun `500_000_000 nanos with 100ms frame returns frame 5`() {
            val frame = calculator.nanosToLogicalFrame(500_000_000)
            assertThat(frame).isEqualTo(5)
        }
    }

    @Nested
    @DisplayName("Time Conversion Functions")
    inner class TimeConversions {

        @Test
        fun `NANOS_PER_MS constant equals 1_000_000`() {
            assertThat(FrameCalculatorService.NANOS_PER_MS).isEqualTo(1_000_000L)
        }

        @Test
        fun `NANOS_PER_SECOND constant equals 1_000_000_000`() {
            assertThat(FrameCalculatorService.NANOS_PER_SECOND).isEqualTo(1_000_000_000L)
        }

        @Nested
        @DisplayName("nanosToMs")
        inner class NanosToMs {

            @Test
            fun `round-trip 1000ms to nanos and back`() {
                val originalMs = 1000L
                val nanos = calculator.msToNanos(originalMs)
                val backToMs = calculator.nanosToMs(nanos)
                assertThat(backToMs).isEqualTo(originalMs)
            }

            @Test
            fun `0 nanos returns 0 ms`() {
                assertThat(calculator.nanosToMs(0)).isEqualTo(0)
            }

            @Test
            fun `very large nanos value converts correctly`() {
                val oneHourNanos = 60L * 60 * 1_000_000_000
                val ms = calculator.nanosToMs(oneHourNanos)
                assertThat(ms).isEqualTo(60 * 60 * 1000L)
            }
        }

        @Nested
        @DisplayName("msToNanos")
        inner class MsToNanos {

            @Test
            fun `0 ms returns 0 nanos`() {
                assertThat(calculator.msToNanos(0)).isEqualTo(0)
            }

            @Test
            fun `1 ms returns 1_000_000 nanos`() {
                assertThat(calculator.msToNanos(1)).isEqualTo(1_000_000L)
            }

            @Test
            fun `1000 ms returns 1_000_000_000 nanos`() {
                assertThat(calculator.msToNanos(1000)).isEqualTo(1_000_000_000L)
            }
        }

        @Nested
        @DisplayName("nanosToSeconds")
        inner class NanosToSeconds {

            @Test
            fun `1_000_000_000 nanos returns 1_0 seconds`() {
                assertThat(calculator.nanosToSeconds(1_000_000_000)).isEqualTo(1.0)
            }

            @Test
            fun `500_000_000 nanos returns 0_5 seconds`() {
                assertThat(calculator.nanosToSeconds(500_000_000)).isEqualTo(0.5)
            }

            @Test
            fun `0 nanos returns 0_0 seconds`() {
                assertThat(calculator.nanosToSeconds(0)).isEqualTo(0.0)
            }

            @Test
            fun `fractional precision is maintained`() {
                val nanos = 123_456_789L
                val seconds = calculator.nanosToSeconds(nanos)
                assertThat(seconds).isEqualTo(0.123456789, org.assertj.core.api.Assertions.within(0.000000001))
            }
        }
    }
}**/