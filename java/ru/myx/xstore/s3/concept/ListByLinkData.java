package ru.myx.xstore.s3.concept;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.function.Function;

import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae3.base.BaseArray;
import ru.myx.ae3.base.BaseArrayAdvanced;
import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.control.ControlBasic;
import ru.myx.ae3.exec.ExecProcess;
import ru.myx.ae3.exec.ExecStateCode;
import ru.myx.ae3.exec.ResultHandler;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.report.Report;
import ru.myx.util.PublicCloneable;

/** Title: Xml Document Management for WSM3 Description: Copyright: Copyright (c) 2001
 *
 * @author Alexander I. Kharitchev
 * @version 1.0
 * @param <R>
 *            item class */
public final class ListByLinkData<R extends ControlBasic<?>> extends BaseHostEmpty implements BaseArrayAdvanced<R>, RandomAccess, List<R>, PublicCloneable {
	
	private final class Itr implements Iterator<R> {
		
		private int cursor = 0;
		
		Itr() {
			
			//
		}
		
		@Override
		public final boolean hasNext() {
			
			return this.cursor != ListByLinkData.this.size();
		}
		
		@Override
		public final R next() {
			
			try {
				return ListByLinkData.this.get(this.cursor++);
			} catch (final IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}
		}
		
		@Override
		public void remove() {
			
			throw new UnsupportedOperationException("Unmodifiable!");
		}
	}
	
	private final class ListItr implements ListIterator<R> {
		
		private int cursor = 0;
		
		ListItr(final int index) {
			
			this.cursor = index;
		}
		
		@Override
		public final void add(final R e) {
			
			throw new UnsupportedOperationException("Unmodifiable!");
		}
		
		@Override
		public final boolean hasNext() {
			
			return this.cursor != ListByLinkData.this.size();
		}
		
		@Override
		public boolean hasPrevious() {
			
			return this.cursor != 0;
		}
		
		@Override
		public final R next() {
			
			try {
				return ListByLinkData.this.get(this.cursor++);
			} catch (final IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}
		}
		
		@Override
		public int nextIndex() {
			
			return this.cursor;
		}
		
		@Override
		public R previous() {
			
			try {
				return ListByLinkData.this.get(--this.cursor);
			} catch (final IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}
		}
		
		@Override
		public int previousIndex() {
			
			return this.cursor - 1;
		}
		
		@Override
		public void remove() {
			
			throw new UnsupportedOperationException("Unmodifiable!");
		}
		
		@Override
		public final void set(final R e) {
			
			throw new UnsupportedOperationException("Unmodifiable!");
		}
	}
	
	private final LinkData[] ids;
	
	private final Function<String, ? extends R> storage;
	
	private final int start;
	
	private final int end;
	
	/** @param ids
	 * @param storage
	 */
	public ListByLinkData(final LinkData[] ids, final Function<String, ? extends R> storage) {
		
		this.ids = ids;
		this.storage = storage;
		this.start = 0;
		this.end = ids.length;
	}
	
	private ListByLinkData(final LinkData[] ids, final int start, final int end, final Function<String, ? extends R> storage) {
		
		this.ids = ids;
		this.storage = storage;
		this.start = start;
		this.end = end;
	}
	
	@Override
	public final void add(final int index, final R element) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean add(final R e) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean addAll(final Collection<? extends R> c) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean addAll(final int index, final Collection<? extends R> c) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public BaseArrayAdvanced<?> baseArraySlice(final int start) {
		
		final int length = this.end - this.start;
		final int fromIndex = BaseArray.genericDefaultSliceGetStartIndex(start, length);
		
		/** It is safe cause it is not Writable nor Dynamic */
		return new ListByLinkData<>(this.ids, fromIndex, length, this.storage);
	}
	
	@Override
	public BaseArrayAdvanced<?> baseArraySlice(final int start, final int end) {
		
		final int length = this.end - this.start;
		final int fromIndex = BaseArray.genericDefaultSliceGetStartIndex(start, length);
		final int toIndex = BaseArray.genericDefaultSliceGetEndIndex(end, length);
		
		/** It is safe cause it is not Writable nor Dynamic */
		return new ListByLinkData<>(this.ids, fromIndex, toIndex, this.storage);
	}
	
	@Override
	public String baseClass() {
		
		return "ListByLinkData";
	}
	
	@Override
	public boolean baseContains(final BaseObject value) {
		
		return this.contains(value);
	}
	
	@Override
	public BaseObject baseGet(final int index, final BaseObject defaultValue) {
		
		if (index < 0) {
			return defaultValue;
		}
		{
			final int size = this.end - this.start;
			if (index >= size) {
				return defaultValue;
			}
		}
		{
			final R item = this.get(index);
			return item == null
				? defaultValue
				: item;
		}
	}
	
	@Override
	public BaseObject baseGetFirst(final BaseObject defaultValue) {
		
		{
			final int size = this.end - this.start;
			if (0 >= size) {
				return defaultValue;
			}
		}
		{
			final R item = this.get(this.start);
			return item == null
				? defaultValue
				: item;
		}
	}
	
	@Override
	public BaseObject baseGetLast(final BaseObject defaultValue) {
		
		{
			final int size = this.end - this.start;
			if (0 >= size) {
				return defaultValue;
			}
		}
		{
			final R item = this.get(this.end - 1);
			return item == null
				? defaultValue
				: item;
		}
	}
	
	@Override
	public Iterator<? extends BaseObject> baseIterator() {
		
		return new Itr();
	}
	
	@Override
	public final void clear() {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public ListByLinkData<R> clone() {
		
		/** read only array, no need to clone it */
		return new ListByLinkData<>(this.ids, this.start, this.end, this.storage);
	}
	
	@Override
	public final boolean contains(final Object o) {
		
		return this.indexOf(o) >= 0;
	}
	
	@Override
	public final boolean containsAll(final Collection<?> c) {
		
		final Iterator<?> e = c.iterator();
		while (e.hasNext()) {
			if (!this.contains(e.next())) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public final boolean equals(final Object o) {
		
		if (o == this) {
			return true;
		}
		if (!(o instanceof ListByLinkData<?>)) {
			return false;
		}
		final ListByLinkData<?> list = (ListByLinkData<?>) o;
		
		final int start1 = this.start;
		final int start2 = list.start;
		final int end1 = this.end;
		final int end2 = list.end;
		
		if (end1 - start1 != end2 - start2) {
			return false;
		}
		
		final LinkData[] ids1 = this.ids;
		final LinkData[] ids2 = list.ids;
		
		for (int i = end1 - start1 - 1; i >= 0; --i) {
			final Object o1 = ids1[start1 + i];
			final Object o2 = ids2[start2 + i];
			if (o1 != o2 && !o1.equals(o2)) {
				return false;
			}
		}
		
		return true;
	}
	
	@Override
	public final R get(final int index) {
		
		final String guid = this.ids[this.start + index].lnkId;
		try {
			final R result = this.storage.apply(guid);
			if (result == null) {
				Report.event(//
						"S3ENTRY", //
						"List item disappeared (Child in tree but not accessible)",
						Format.Throwable.toText(//
								new RuntimeException("guid=" + guid + ", listLength=" + this.length())//
						)//
				);
			}
			return result;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final int hashCode() {
		
		final int start = this.start;
		final int end = this.end;
		int hashCode = 1;
		final Object[] ids = this.ids;
		for (int i = start; i < end; ++i) {
			final Object obj = ids[i];
			hashCode = 31 * hashCode + (obj == null
				? 0
				: obj.hashCode());
		}
		return hashCode;
	}
	
	@Override
	public final int indexOf(final Object o) {
		
		if (o != null && o instanceof BaseEntry<?>) {
			final String guid = ((BaseEntry<?>) o).getGuid();
			for (int i = this.start; i < this.end; ++i) {
				if (guid.equals(this.ids[i].lnkId)) {
					return i;
				}
			}
			
		}
		return -1;
	}
	
	@Override
	public final boolean isEmpty() {
		
		return this.size() == 0;
	}
	
	@Override
	public final Iterator<R> iterator() {
		
		return new Itr();
	}
	
	@Override
	public int lastIndexOf(final Object o) {
		
		if (o != null && o instanceof BaseEntry<?>) {
			final String guid = ((BaseEntry<?>) o).getGuid();
			for (int i = this.end - 1; i >= this.start; --i) {
				if (guid.equals(this.ids[i].lnkId)) {
					return i;
				}
			}
			
		}
		return -1;
	}
	
	@Override
	public final int length() {
		
		return this.end - this.start;
	}
	
	@Override
	public final ListIterator<R> listIterator() {
		
		return this.listIterator(0);
	}
	
	@Override
	public final ListIterator<R> listIterator(final int index) {
		
		if (index < 0 || index > this.size()) {
			throw new IndexOutOfBoundsException("Index: " + index);
		}
		return new ListItr(index);
	}
	
	@Override
	public final R remove(final int index) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean remove(final Object o) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean removeAll(final Collection<?> c) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final boolean retainAll(final Collection<?> c) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final R set(final int index, final R element) {
		
		throw new UnsupportedOperationException("Unmodifiable!");
	}
	
	@Override
	public final int size() {
		
		return this.end - this.start;
	}
	
	@Override
	public final List<R> subList(final int startPosition, final int endPosition) {
		
		return new ListByLinkData<>(this.ids, this.start + startPosition, this.start + endPosition, this.storage);
	}
	
	@Override
	public final Object[] toArray() {
		
		final int size = this.size();
		final Object[] result = new Object[size];
		for (int i = size - 1; i >= 0; --i) {
			result[i] = this.get(i);
		}
		return result;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public final <T> T[] toArray(final T[] a) {
		
		final int size = this.size();
		final T[] result = a.length >= size
			? a
			: (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
		
		for (int i = size - 1; i >= 0; --i) {
			result[i] = (T) this.get(i);
		}
		return result;
	}
	
	@Override
	public final String toString() {
		
		final int start = this.start;
		final int end = this.end;
		final int size = end - start;
		final StringBuilder builder = new StringBuilder((size + 1) * 48);
		builder.append('[');
		if (size > 0) {
			final LinkData[] ids = this.ids;
			builder.append(ids[start]);
			if (size > 1) {
				for (int i = start + 1; i < end; ++i) {
					builder.append(',');
					builder.append(ids[i].lnkId);
				}
			}
		}
		builder.append(']');
		return builder.toString();
	}
	
	@Override
	public ExecStateCode vmPropertyRead(final ExecProcess ctx, final int index, final BaseObject originalIfKnown, final BaseObject defaultValue, final ResultHandler store) {
		
		return store.execReturn(ctx, this.baseGet(index, defaultValue));
	}
}
