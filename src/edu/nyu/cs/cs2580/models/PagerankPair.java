package edu.nyu.cs.cs2580.models;

import java.io.Serializable;

/**
 * Created by chenprice on 11/15/14.
 */
public class PagerankPair implements Serializable{
    private Short docid;
    private Double rank;

    public PagerankPair(Short did, Double r){
        docid = did;
        rank = r;
    }

    public Short getDocid() {
        return docid;
    }

    public void setDocid(short docid) {
        this.docid = docid;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    @Override
    public String toString(){
        return "(" + docid +"," + rank + ")";
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PagerankPair other = (PagerankPair) obj;
        if (docid == null)
        {
            if (other.docid != null)
                return false;
        }
        else if (!docid.equals(other.docid))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((docid == null) ? 0 : docid.hashCode());
        return result;
    }
}
