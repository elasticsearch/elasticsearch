/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.util.gcommon.collect;

import org.elasticsearch.util.gcommon.annotations.GwtCompatible;

import java.io.Serializable;

import javax.annotation.Nullable;

/** An ordering that treats {@code null} as greater than all other values. */
@GwtCompatible(serializable = true)
final class NullsLastOrdering<T> extends Ordering<T> implements Serializable {
  final Ordering<? super T> ordering;

  NullsLastOrdering(Ordering<? super T> ordering) {
    this.ordering = ordering;
  }

  public int compare(T left, T right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return LEFT_IS_GREATER;
    }
    if (right == null) {
      return RIGHT_IS_GREATER;
    }
    return ordering.compare(left, right);
  }

  @Override public <S extends T> Ordering<S> reverse() {
    // ordering.reverse() might be optimized, so let it do its thing
    return ordering.reverse().nullsFirst();
  }

  @Override public <S extends T> Ordering<S> nullsFirst() {
    return ordering.nullsFirst();
  }

  @SuppressWarnings("unchecked") // still need the right way to explain this
  @Override public <S extends T> Ordering<S> nullsLast() {
    return (Ordering) this;
  }

  @Override public boolean equals(@Nullable Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof NullsLastOrdering) {
      NullsLastOrdering<?> that = (NullsLastOrdering<?>) object;
      return this.ordering.equals(that.ordering);
    }
    return false;
  }

  @Override public int hashCode() {
    return ordering.hashCode() ^ -921210296; // meaningless
  }

  @Override public String toString() {
    return ordering + ".nullsLast()";
  }

  private static final long serialVersionUID = 0;
}
