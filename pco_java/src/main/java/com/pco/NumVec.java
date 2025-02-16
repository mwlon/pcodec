package com.pco;

public class NumVec {
    private final int dtype;
    private final Object data;
    
    private NumVec(int dtype, Object data) {
        this.dtype = dtype;
        this.data = data;
    }

    public static NumVec fromF16(short[] data) {
        return new NumVec(0, data);
    }

    public static NumVec fromF32(float[] data) {
        return new NumVec(1, data);
    }

    public static NumVec fromF64(double[] data) {
        return new NumVec(2, data);
    }

    public static NumVec fromI16(short[] data) {
        return new NumVec(3, data);
    }

    public static NumVec fromI32(int[] data) {
        return new NumVec(4, data);
    }

    public static NumVec fromI64(long[] data) {
        return new NumVec(5, data);
    }

    public static NumVec fromU16(short[] data) {
        return new NumVec(6, data);
    }

    public static NumVec fromU32(int[] data) {
        return new NumVec(7, data);
    }

    public static NumVec fromU64(long[] data) {
        return new NumVec(8, data);
    }

    public short[] asF16() {
        if (dtype != 0) throw new InvalidTypeException("f16");
        return (short[]) data;
    }

    public float[] asF32() {
        if (dtype != 1) throw new InvalidTypeException("f32");
        return (float[]) data;
    }

    public double[] asF64() {
        if (dtype != 2) throw new InvalidTypeException("f64");
        return (double[]) data;
    }

    public short[] asI16() {
        if (dtype != 3) throw new InvalidTypeException("i16");
        return (short[]) data;
    }

    public int[] asI32() {
        if (dtype != 4) throw new InvalidTypeException("i32");
        return (int[]) data;
    }

    public long[] asI64() {
        if (dtype != 5) throw new InvalidTypeException("i64");
        return (long[]) data;
    }

    public short[] asU16() {
        if (dtype != 6) throw new InvalidTypeException("u16");
        return (short[]) data;
    }

    public int[] asU32() {
        if (dtype != 7) throw new InvalidTypeException("u32");
        return (int[]) data;
    }

    public long[] asU64() {
        if (dtype != 8) throw new InvalidTypeException("u64");
        return (long[]) data;
    }

    // Package-private getters for JNI
    int getDtype() { return dtype; }
    Object getData() { return data; }
}