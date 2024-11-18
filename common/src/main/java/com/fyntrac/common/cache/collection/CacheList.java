package com.fyntrac.common.cache.collection;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A generic cache list that allows adding elements and retrieving chunks of elements.
 *
 * @param <T> the type of elements in this cache list
 */
public class CacheList<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = -6503013787584293223L;

    private List<T> list;

    /**
     * Constructs an empty CacheList.
     */
    public CacheList() {
        this.list = new ArrayList<>(0);
    }

    /**
     * Adds an element to the list.
     *
     * @param o the element to add
     */
    public void add(T o) {
        this.list.add(o);
    }

    /**
     * Adds all elements from the specified collection to the list.
     *
     * @param collection the collection of elements to add
     */
    public void addAll(Collection<? extends T> collection) {
        if (collection != null) {
            this.list.addAll(collection);
        }
    }

    /**
     * Retrieves the internal list.
     *
     * @return the list of elements
     */
    public List<T> getList() {
        return this.list;
    }

    /**
     * Retrieves a chunk of the list.
     *
     * @param chunkSize  The size of each chunk.
     * @param chunkIndex The index of the chunk to retrieve (0-based).
     * @return A sublist containing the specified chunk, or an empty list if the index is out of bounds.
     */
    public List<T> getChunk(int chunkSize, int chunkIndex) {
        if (chunkSize <= 0 || chunkIndex < 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0 and chunk index must be non-negative.");
        }

        int fromIndex = chunkIndex * chunkSize;
        if (fromIndex >= list.size()) {
            return new ArrayList<>(); // Return an empty list if the index is out of bounds
        }

        int toIndex = Math.min(fromIndex + chunkSize, list.size());
        return new ArrayList<>(list.subList(fromIndex, toIndex));
    }

    /**
     * Calculates the total number of chunks based on the list size and chunk size.
     *
     * @param chunkSize The size of each chunk.
     * @return The total number of chunks.
     */
    public int getTotalChunks(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0.");
        }
        return (int) Math.ceil((double) list.size() / chunkSize);
    }
}