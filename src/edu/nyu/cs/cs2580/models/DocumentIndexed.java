package edu.nyu.cs.cs2580.models;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
    private static final long serialVersionUID = 9184892508124423115L;
    public long size;

    public DocumentIndexed(String url, long docid, long size, String title) {
        super(docid);
        this.setUrl(url);
        this.size = size;
        this.setTitle(title);
    }
}
