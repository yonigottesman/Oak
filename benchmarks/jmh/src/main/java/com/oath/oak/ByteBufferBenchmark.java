package com.oath.oak;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ByteBufferBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"ONHEAP","OFFHEAP"})
        String heap;

        ByteBuffer byteBuffer;


        int bytes = 1024*1024*1024;

        @Param({"1000000"})
        int operations;

        int[] randomIndex;

        @Setup()
        public void setup() {
            if (heap.equals("ONHEAP")) {
                byteBuffer = ByteBuffer.allocate(bytes);
            } else {
                byteBuffer = ByteBuffer.allocateDirect(bytes);
            }

            Random r = new Random();
            randomIndex = new int[operations];
            for (int i = 0; i < operations; ++i) {
                randomIndex[i] = r.nextInt(bytes);
            }
        }
    }


    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1)
    @Threads(1)
    @Benchmark
    public void put(Blackhole blackhole, BenchmarkState state) {
        for (int i=0; i < state.operations; ++i) {
            blackhole.consume(state.byteBuffer.put(state.randomIndex[i], (byte) i));
        }
    }

    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1)
    @Threads(1)
    @Benchmark
    public void get(Blackhole blackhole, BenchmarkState state) {
        for (int i=0; i < state.operations; ++i) {
            blackhole.consume(state.byteBuffer.get(state.randomIndex[i]));
        }
    }

    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1)
    @Threads(1)
    @Benchmark
    public void compare(Blackhole blackhole, BenchmarkState state) {
        for (int i=0; i < state.operations; ++i) {
            byte value = state.byteBuffer.get(state.randomIndex[i]);
            blackhole.consume(Byte.compare(value, (byte)i));
        }
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ByteBufferBenchmark.class.getSimpleName())
                .forks(0)
                .threads(1)
                .build();

        new Runner(opt).run();
    }

}
