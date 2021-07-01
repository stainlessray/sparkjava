package com.virtualpairprogrammers.sections_one_thru_eight;

public class IntegerWithSquareRoot {

    private int originalNumber;
    private Double squareRoot;

    public IntegerWithSquareRoot(int i) {
        this.originalNumber = i;
        this.squareRoot = Math.sqrt(originalNumber);
    }
}
