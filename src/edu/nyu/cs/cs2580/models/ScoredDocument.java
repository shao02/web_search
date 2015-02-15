package edu.nyu.cs.cs2580.models;

/**
 * Document with score.
 *
 * @author fdiaz
 * @author congyu
 */
public class ScoredDocument implements Comparable<ScoredDocument> {
    private Document _doc;
    private double _score;

    public ScoredDocument(Document doc, double score) {
        _doc = doc;
        _score = score;
    }

    public double getScore() {
        return _score;
    }

    public void setScore(double score) {
        _score = score;
    }
    
    public long getDocumentId() {
        return _doc._docid;
    }

    public Document getDoc() { return _doc; };

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(_doc._docid).append("\t");
        buf.append(_doc.getTitle()).append("\t");
        buf.append(_score).append('\t');
        buf.append(_doc.getNumViews()).append('\t');
        buf.append(_doc.getPageRank());
        return buf.toString();
    }

    /**
     * @CS2580: Student should implement {@code asHtmlResult} for final project.
     */
    public String asHtmlResult() {
        return  "<tr>" +
                    "<td>" + _doc._docid + "</td>" +
                    "<td>" + _doc.getTitle() + "</td>" +
                    "<td>" + _score + "</td>" +
                    "<td>" + _doc.getNumViews() + "</td>" +
                    "<td>" + _doc.getPageRank() + "</td>" +
                "</tr>";
    }

    @Override
    public int compareTo(ScoredDocument o) {
        if (this._score == o._score) {
            return 0;
        }
        return (this._score > o._score) ? 1 : -1;
    }
}
