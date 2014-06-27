/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package knoelab.classification;

import jsr166y.*;
import java.util.*;

import extra166y.ParallelArray;

class ParallelArraySortDemo {

    static final Random rng = new Random();
    static final long NPS = (1000L * 1000 * 1000);

    public static void main(String[] args) throws Exception {
    	if(args.length != 2)
    		throw new Exception("Provide 2 args -- total numbers and multiplier");
    	
//        int n = 1 << 20;
    	int n = Integer.parseInt(args[0]);
    	int multiplier = Integer.parseInt(args[1]);
        int reps = 10;
        System.out.println("1 << 20: " + (1 << 20));
        System.out.printf("Sorting %d Longs, %d replications\n", n, reps);
        Long[] a = new Long[n];
        randomFill(a);

        for (int i = 0; i < reps; ++i) {
            long last = System.nanoTime();
            java.util.Arrays.sort(a);
            double elapsed = (double)(System.nanoTime() - last) / NPS;
            System.out.printf("java.util.Arrays.sort time:  %7.3f\n", elapsed);
            checkSorted(a);
            shuffle(a);
            //            System.gc();
        }
        System.out.println();
        final int MAX_THREADS = Runtime.getRuntime().availableProcessors() * multiplier;
        System.out.println("No threads: " + MAX_THREADS);
        ForkJoinPool fjpool = new ForkJoinPool(MAX_THREADS);
        ParallelArray<Long> pa = ParallelArray.createUsingHandoff(a, fjpool);
        for (int i = 0; i < reps; ++i) {
            long last = System.nanoTime();
            pa.sort();
            double elapsed = (double)(System.nanoTime() - last) / NPS;
            System.out.printf("Parallel sort time:        %7.3f\n", elapsed);
            checkSorted(a);
            shuffle(a);
            //            System.gc();
        }
        System.out.println(fjpool);
        fjpool.shutdown();
    }

    static void checkSorted(Long[] a) {
        int n = a.length;
        for (int i = 0; i < n - 1; i++) {
            if (a[i].compareTo(a[i+1]) > 0) {
                throw new Error("Unsorted at " + i + ": " + a[i] + " / " + a[i+1]);
            }
        }
    }

    static void randomFill(Long[] a) {
        for (int i = 0; i < a.length; ++i)
            a[i] = new Long(rng.nextLong());
    }

    static void shuffle(Long[] a) {
        int n = a.length;
        for (int i = n; i > 1; --i) {
            int r = rng.nextInt(i);
            Long t = a[i-1];
            a[i-1] = a[r];
            a[r] = t;
        }
    }


}
