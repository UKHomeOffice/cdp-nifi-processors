package com.pontusnetworks.utils;

import java.util.*;

public class ConcurrentLinkedHashMap<K, V> implements Map<K, V>
{ // NOPMD
  private transient Map<K, V> internalMap = null;

  public ConcurrentLinkedHashMap()
  {
    this(100);
  }

  public ConcurrentLinkedHashMap(final int maxSize)
  {
    internalMap = Collections.synchronizedMap(new LruCache<K, V>(maxSize));
  }

  public void removeHandler(Map.Entry<K, V> eldest)
  {

  }

  @Override public int size()
  {

    return internalMap.size();
  }

  @Override public boolean isEmpty()
  {

    return internalMap.isEmpty();
  }

  @Override public boolean containsKey(final Object key)
  {
    return internalMap.containsKey(key);
  }

  @Override public boolean containsValue(final Object value)
  {

    return internalMap.containsValue(value);
  }

  @Override public V get(final Object key)
  {

    return (V) internalMap.get(key);
  }

  @Override public V put(final K key, final V value)
  {
    return internalMap.put(key, value);
  }

  @Override public V remove(final Object key)
  {

    return internalMap.remove(key);
  }

  @Override public void putAll(final Map<? extends K, ? extends V> map)
  {
    internalMap.putAll(map);

  }

  @Override public void clear()
  {
    internalMap.clear();
  }

  @Override public Set<K> keySet()
  {

    return internalMap.keySet();
  }

  @Override public Collection<V> values()
  {

    return internalMap.values();
  }

  @Override public Set<java.util.Map.Entry<K, V>> entrySet()
  {

    return internalMap.entrySet();
  }

  private class LruCache<A, B> extends LinkedHashMap<A, B>
  {
    /**
     *
     */
    private static final long serialVersionUID = -3472281467319974671L;
    private transient final int maxEntries;

    public LruCache(final int maxEntries)
    {
      super(maxEntries + 1, 1.0f, true);
      this.maxEntries = maxEntries;
    }

    /**
     * Returns <tt>true</tt> if this <code>LruCache</code> has more entries than
     * the maximum specified when it was created.
     * <p>
     * <p>
     * This method <em>does not</em> modify the underlying <code>Map</code>; it
     * relies on the implementation of <code>LinkedHashMap</code> to do that,
     * but that behavior is documented in the JavaDoc for
     * <code>LinkedHashMap</code>.
     * </p>
     *
     * @param eldest the <code>Entry</code> in question; this implementation doesn't
     *               care what it is, since the implementation is only dependent on
     *               the size of the cache
     * @return <tt>true</tt> if the oldest
     * @see java.util.LinkedHashMap#removeEldestEntry(Map.Entry)
     */
    @SuppressWarnings("unchecked") @Override protected boolean removeEldestEntry(final Map.Entry<A, B> eldest)
    {
      boolean removeIt = super.size() > maxEntries;
      if (removeIt)
      {
        removeHandler((java.util.Map.Entry<K, V>) eldest);
      }
      return removeIt;
    }
  }

}
