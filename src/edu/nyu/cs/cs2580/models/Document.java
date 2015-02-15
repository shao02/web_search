package edu.nyu.cs.cs2580.models;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 * The basic implementation of a Document.  Only the most basic information are
 * maintained in this class. Subclass should implement additional information
 * for display or ranking, such as snippet, term vectors, anchors, etc.
 *
 * @author fdiaz
 * @author congyu
 */
public class Document implements Serializable {
    private static final long serialVersionUID = -539495106357836976L;

    public long _docid;

    // Basic information for display
    private String _title = "";
    private String _url = "";

    // Basic information for ranking
    private float _pageRank = 0.0f;
    private int _numViews = 0;

    public Document(long docid) {
        _docid = docid;
    }

    public String getTitle() {
        return _title;
    }

    public void setTitle(String title) {
        this._title = title;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        this._url = url;
    }

    public float getPageRank() {
        return _pageRank;
    }

    public void setPageRank(float pageRank) {
        this._pageRank = pageRank;
    }

    public int getNumViews() {
        return _numViews;
    }

    public void setNumViews(int numViews) {
        this._numViews = numViews;
    }

    public String toString() {
        return Long.toString(_docid);
    }
}
