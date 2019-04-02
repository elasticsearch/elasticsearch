/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.object.HasToString.hasToString;

public class TimeValueTests extends ESTestCase {

    public void testSimple() {
        assertThat(TimeUnit.MILLISECONDS.toMillis(10), equalTo(new TimeValue(10, TimeUnit.MILLISECONDS).millis()));
        assertThat(TimeUnit.MICROSECONDS.toMicros(10), equalTo(new TimeValue(10, TimeUnit.MICROSECONDS).micros()));
        assertThat(TimeUnit.SECONDS.toSeconds(10), equalTo(new TimeValue(10, TimeUnit.SECONDS).seconds()));
        assertThat(TimeUnit.MINUTES.toMinutes(10), equalTo(new TimeValue(10, TimeUnit.MINUTES).minutes()));
        assertThat(TimeUnit.HOURS.toHours(10), equalTo(new TimeValue(10, TimeUnit.HOURS).hours()));
        assertThat(TimeUnit.DAYS.toDays(10), equalTo(new TimeValue(10, TimeUnit.DAYS).days()));
    }

    public void testToString() {
        assertThat("10ms", equalTo(new TimeValue(10, TimeUnit.MILLISECONDS).toString()));
        assertThat("1.5s", equalTo(new TimeValue(1533, TimeUnit.MILLISECONDS).toString()));
        assertThat("1.5m", equalTo(new TimeValue(90, TimeUnit.SECONDS).toString()));
        assertThat("1.5h", equalTo(new TimeValue(90, TimeUnit.MINUTES).toString()));
        assertThat("1.5d", equalTo(new TimeValue(36, TimeUnit.HOURS).toString()));
        assertThat("1000d", equalTo(new TimeValue(1000, TimeUnit.DAYS).toString()));
    }

    public void testMinusOne() {
        assertThat(new TimeValue(-1).nanos(), lessThan(0L));
    }

    public void testParseTimeValue() {
        // Space is allowed before unit:
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10 ms", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10ms", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10 MS", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10MS", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10 s", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10s", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10 S", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10S", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10 m", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10m", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10 h", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10h", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10 H", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10H", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 d", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10d", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 D", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10D", null, "test"));

        // Time values of months should throw an exception as months are not
        // supported. Note that this is the only unit that is not case sensitive
        // as `m` is the only character that is overloaded in terms of which
        // time unit is expected between the upper and lower case versions
        expectThrows(IllegalArgumentException.class, () -> {
            TimeValue.parseTimeValue("10 M", null, "test");
        });
        expectThrows(IllegalArgumentException.class, () -> {
            TimeValue.parseTimeValue("10M", null, "test");
        });

        final int length = randomIntBetween(0, 8);
        final String zeros = new String(new char[length]).replace('\0', '0');
        assertTrue(TimeValue.parseTimeValue("-" + zeros + "1", null, "test") == TimeValue.MINUS_ONE);
        assertTrue(TimeValue.parseTimeValue(zeros + "0", null, "test") == TimeValue.ZERO);
    }

    public void testParseFractional() {
        assertEquals(new TimeValue(36, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("1.5d", "test"));
        assertEquals(new TimeValue(130464, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("1.51d", "test"));
        assertEquals(new TimeValue(1584, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("1.1d", "test"));
        assertEquals(new TimeValue(66, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("1.1h", "test"));
        assertEquals(new TimeValue(66, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("1.1m", "test"));
        assertEquals(new TimeValue(1100, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("1.1s", "test"));
        assertEquals(new TimeValue(1200, TimeUnit.MICROSECONDS),
                     TimeValue.parseTimeValue("1.2ms", "test"));
        assertEquals(new TimeValue(1200, TimeUnit.NANOSECONDS),
                     TimeValue.parseTimeValue("1.2micros", "test"));
    }

    public void testFractionalToString() {
        assertEquals("1.5d", TimeValue.parseTimeValue("1.5d", "test").toString());
        assertEquals("1.5d", TimeValue.parseTimeValue("1.51d", "test").toString());
        assertEquals("1.1d", TimeValue.parseTimeValue("1.1d", "test").toString());
        assertEquals("1.1h", TimeValue.parseTimeValue("1.1h", "test").toString());
        assertEquals("1.1m", TimeValue.parseTimeValue("1.1m", "test").toString());
        assertEquals("1.1s", TimeValue.parseTimeValue("1.1s", "test").toString());
        assertEquals("1.2ms", TimeValue.parseTimeValue("1.2ms", "test").toString());
        assertEquals("1.2micros", TimeValue.parseTimeValue("1.2micros", "test").toString());
    }

    private static final String OVERFLOW_VALUES_NOT_SUPPORTED = "Values greater than 9223372036854775807 nanoseconds are not supported";
    private static final String UNDERFLOW_VALUES_NOT_SUPPORTED = "Values less than -9223372036854775808 nanoseconds are not supported";

    public void testParseValueTooLarge() {
        final String longValue = "106752d";
        final IllegalArgumentException longOverflow =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(longValue, ""));
        assertThat(longOverflow, hasToString(containsString(OVERFLOW_VALUES_NOT_SUPPORTED)));
        assertThat(longOverflow, hasToString(endsWith(longValue)));
    }

    public void testParseValueTooSmall() {
        final String longValue = "-106752d";
        final IllegalArgumentException longOverflow =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(longValue, ""));
        assertThat(longOverflow, hasToString(containsString(UNDERFLOW_VALUES_NOT_SUPPORTED)));
        assertThat(longOverflow, hasToString(endsWith(longValue)));
    }

    public void testParseFractionalValueTooLarge() {
        final String fractionalValue = "106751.991167302d";
        final IllegalArgumentException fractionalOverflow =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(fractionalValue, ""));
        assertThat(fractionalOverflow, hasToString(containsString(OVERFLOW_VALUES_NOT_SUPPORTED)));
        assertThat(fractionalOverflow, hasToString(endsWith(fractionalValue)));
    }

    public void testParseFractionalValueTooSmall() {
        final String fractionalValue = "-106751.991167302d";
        final IllegalArgumentException fractionalOverflow =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(fractionalValue, ""));
        assertThat(fractionalOverflow, hasToString(containsString(UNDERFLOW_VALUES_NOT_SUPPORTED)));
        assertThat(fractionalOverflow, hasToString(endsWith(fractionalValue)));
    }

    public void testRoundTrip() {
        final String s = randomTimeValue();
        assertThat(TimeValue.parseTimeValue(s, null, "test").getStringRep(), equalTo(s));
        final TimeValue t = new TimeValue(randomIntBetween(1, 128), randomFrom(TimeUnit.values()));
        assertThat(TimeValue.parseTimeValue(t.getStringRep(), null, "test"), equalTo(t));
    }

    private static final String FRACTIONAL_NANO_TIME_VALUES_ARE_NOT_SUPPORTED = "fractional nanosecond values are not supported";

    public void testNonFractionalTimeValues() {
        final String s = randomAlphaOfLength(10) + randomTimeUnit();
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(s, null, "test"));
        assertThat(e, hasToString(containsString("failed to parse [" + s + "]")));
        assertThat(e, not(hasToString(containsString(FRACTIONAL_NANO_TIME_VALUES_ARE_NOT_SUPPORTED))));
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testFractionalNanoValues() {
        double value;
        do {
            value = randomDouble();
        } while (value == 0);
        final String s = (randomIntBetween(0, 128) + value) + "nanos";
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> TimeValue.parseTimeValue(s, null, "test"));
        assertThat(e, hasToString(containsString("failed to parse [" + s + "]")));
        assertThat(e, hasToString(containsString(FRACTIONAL_NANO_TIME_VALUES_ARE_NOT_SUPPORTED)));
    }

    private String randomTimeUnit() {
        return randomFrom("nanos", "micros", "ms", "s", "m", "h", "d");
    }

    public void testFailOnUnknownUnits() {
        try {
            TimeValue.parseTimeValue("23tw", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testFailOnMissingUnits() {
        try {
            TimeValue.parseTimeValue("42", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testNoDotsAllowed() {
        try {
            TimeValue.parseTimeValue("42ms.", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testToStringRep() {
        assertEquals("-1", new TimeValue(-1).getStringRep());
        assertEquals("10ms", new TimeValue(10, TimeUnit.MILLISECONDS).getStringRep());
        assertEquals("1533ms", new TimeValue(1533, TimeUnit.MILLISECONDS).getStringRep());
        assertEquals("90s", new TimeValue(90, TimeUnit.SECONDS).getStringRep());
        assertEquals("90m", new TimeValue(90, TimeUnit.MINUTES).getStringRep());
        assertEquals("36h", new TimeValue(36, TimeUnit.HOURS).getStringRep());
        assertEquals("1000d", new TimeValue(1000, TimeUnit.DAYS).getStringRep());
    }

    public void testCompareEquality() {
        long randomLong = randomNonNegativeLong();
        TimeUnit randomUnit = randomFrom(TimeUnit.values());
        TimeValue firstValue = new TimeValue(randomLong, randomUnit);
        TimeValue secondValue = new TimeValue(randomLong, randomUnit);
        assertEquals(0, firstValue.compareTo(secondValue));
    }

    public void testCompareValue() {
        long firstRandom = randomNonNegativeLong();
        long secondRandom = randomValueOtherThan(firstRandom, ESTestCase::randomNonNegativeLong);
        TimeUnit unit = randomFrom(TimeUnit.values());
        TimeValue firstValue = new TimeValue(firstRandom, unit);
        TimeValue secondValue = new TimeValue(secondRandom, unit);
        assertEquals(firstRandom > secondRandom, firstValue.compareTo(secondValue) > 0);
        assertEquals(secondRandom > firstRandom, secondValue.compareTo(firstValue) > 0);
    }

    public void testCompareUnits() {
        long number = randomNonNegativeLong();
        TimeUnit randomUnit = randomValueOtherThan(TimeUnit.DAYS, ()->randomFrom(TimeUnit.values()));
        TimeValue firstValue = new TimeValue(number, randomUnit);
        TimeValue secondValue = new TimeValue(number, TimeUnit.DAYS);
        assertTrue(firstValue.compareTo(secondValue) < 0);
        assertTrue(secondValue.compareTo(firstValue) > 0);
    }

    public void testConversionHashCode() {
        TimeValue firstValue = new TimeValue(randomIntBetween(0, Integer.MAX_VALUE), TimeUnit.MINUTES);
        TimeValue secondValue = new TimeValue(firstValue.getSeconds(), TimeUnit.SECONDS);
        assertEquals(firstValue.hashCode(), secondValue.hashCode());
    }
}
