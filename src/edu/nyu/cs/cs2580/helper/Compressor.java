package edu.nyu.cs.cs2580.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Compressor {
    public static int[] deltaEncode(long[] postingList) {
        int index = 0;
        int[] result = new int[postingList.length];
        long baseDocId = 0;
        while (index < postingList.length) {
            long docId = postingList[index];
            docId = docId - baseDocId;
            baseDocId = docId + baseDocId;
            result[index++] = (int) docId;
            long numOccs = postingList[index];
            result[index++] = (int) numOccs;

            long baseIdx = 0;
            for (int i = 0; i < numOccs; i++) {
                long idx = postingList[index];
                idx = idx - baseIdx;
                baseIdx = idx + baseIdx;
                result[index++] = (int) idx;
            }
        }
        return result;
    }

    public static byte[] convertToVBytes(int[] postingList) {
        List<Byte> bytes = new ArrayList<Byte>(postingList.length * 2);
        for (int p : postingList) {
            for (byte b : convertToVBytes(p))
                bytes.add(b);
        }
        byte[] result = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) result[i] = bytes.get(i);
        return result;
    }

    public static byte[] convertToVBytes(int p) {
        List<Byte> resultList = new ArrayList<Byte>();
        do {
            resultList.add((byte) (p & ((1 << 7) - 1)));
            p >>= 7;
        } while (p > 0);
        Collections.reverse(resultList);
        byte b = resultList.get(resultList.size() - 1);
        b |= (1<<7);
        resultList.set(resultList.size() - 1, b);
        byte[] result = new byte[resultList.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = resultList.get(i);
        }
        return result;
    }

    public static int[] decodeVBytes(byte[] postingList) {
        List<Integer> resultList = new ArrayList<Integer>();
        for (int i = 0; i < postingList.length; ) {
            List<Byte> bytes = new ArrayList<Byte>();
            while (true) {
                byte b = postingList[i++];
                if ((b & (1<<7)) == (1<<7)) {
                    bytes.add((byte) (b & ~(1<<7)));
                    break;
                }
                bytes.add(b);
            }
            Collections.reverse(bytes);
            int result = 0;
            for (int j = 0; j < bytes.size(); j++) {
                result += (bytes.get(j)) << (7*j);
            }
            resultList.add(result);
        }
        int[] result = new int[resultList.size()];
        for (int i = 0; i < result.length; i++) result[i] = resultList.get(i);
        return result;
    }
}
