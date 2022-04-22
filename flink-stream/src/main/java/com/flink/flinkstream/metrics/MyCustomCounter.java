package com.flink.flinkstream.metrics;

import org.apache.flink.metrics.Counter;

/**
 * 自定义一个counter，实现counter达到最大值后再清0
 */
public class MyCustomCounter implements Counter {

    private long count;

    private long maxCount;

    private long minCount;

    public MyCustomCounter(long maxCount,long minCount) {
        this.maxCount = maxCount;
        this.minCount = minCount;
    }

    @Override
    public void inc() {
        ++this.count;
        if(this.count>=maxCount){
            this.count = 0;
        }
    }

    @Override
    public void inc(long l) {
        this.count += l;
        if(this.count>=maxCount){
            this.count = 0;
        }
    }

    @Override
    public void dec() {
        --this.count;
        if(this.count<=minCount){
            this.count = 0;
        }
    }

    @Override
    public void dec(long l) {
        this.count -= l;
        if(this.count<=minCount){
            this.count = 0;
        }
    }

    @Override
    public long getCount() {
        return this.count;
    }
}
