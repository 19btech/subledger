package com.fyntrac.common.cache.collection;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

/**
 * A generic cache set that allows adding elements and retrieving chunks of elements.
 * Maintains insertion order like LinkedHashSet.
 *
 * @param <T> the type of elements in this cache set
 */
public class CacheSet<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 2619566877113162005L;

    private final Set<T> set;

    /**
     * Constructs an empty CacheSet that maintains insertion order.
     */
    public CacheSet() {
        this.set = new LinkedHashSet<>(0);
    }

    /**
     * Adds an element to the set if it is not already present.
     *
     * @param o the element to add
     * @return true if the element was added, false if it was already present
     */
    public boolean add(T o) {
        return this.set.add(o);
    }

    /**
     * Adds all elements from the specified collection to the set.
     *
     * @param collection the collection of elements to add
     * @return true if the set changed as a result of the call
     */
    public boolean addAll(Collection<? extends T> collection) {
        if (collection != null) {
            return this.set.addAll(collection);
        }
        return false;
    }

    /**
     * Retrieves the internal set.
     *
     * @return the set of elements
     */
    public Set<T> getSet() {
        return this.set;
    }

    /**
     * Retrieves a chunk of the set as a list (maintaining insertion order).
     *
     * @param chunkSize  The size of each chunk.
     * @param chunkIndex The index of the chunk to retrieve (0-based).
     * @return A list containing the specified chunk, or an empty list if the index is out of bounds.
     */
    public List<T> getChunk(int chunkSize, int chunkIndex) {
        if (chunkSize <= 0 || chunkIndex < 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0 and chunk index must be non-negative.");
        }

        // Convert to list to maintain order
        List<T> orderedList = new ArrayList<>(set);

        int fromIndex = chunkIndex * chunkSize;
        if (fromIndex >= orderedList.size()) {
            return new ArrayList<>(); // Return an empty list if the index is out of bounds
        }

        int toIndex = Math.min(fromIndex + chunkSize, orderedList.size());
        return new ArrayList<>(orderedList.subList(fromIndex, toIndex));
    }

    /**
     * Calculates the total number of chunks based on the set size and chunk size.
     *
     * @param chunkSize The size of each chunk.
     * @return The total number of chunks.
     */
    public int getTotalChunks(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0.");
        }
        return (int) Math.ceil((double) set.size() / chunkSize);
    }

    /**
     * Returns the number of elements in the set.
     *
     * @return the number of elements in the set
     */
    public int size() {
        return set.size();
    }

    /**
     * Checks if the set contains the specified element.
     *
     * @param o element whose presence in the set is to be tested
     * @return true if the set contains the specified element
     */
    public boolean contains(T o) {
        return set.contains(o);
    }

    /**
     * Removes the specified element from the set if it is present.
     *
     * @param o element to be removed from the set
     * @return true if the set contained the specified element
     */
    public boolean remove(T o) {
        return set.remove(o);
    }

    /**
     * Removes all elements from the set.
     */
    public void clear() {
        set.clear();
    }
}