package com.axing._05自定义对象分区;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
public class FlowBean implements Writable {
    private long upFlow; //上行流量
    private long downFlow;//下行流量
    private long sumFlow; //总流量

    // 合并函数
    public static void combine(FlowBean a, FlowBean b) {
        FlowBean combined = new FlowBean();
        combined.upFlow = a.upFlow + b.upFlow;
        combined.downFlow = a.downFlow + b.downFlow;
        combined.sumFlow = combined.upFlow + combined.downFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    // 累加函数
    public void accumulate(FlowBean other) {
        this.upFlow += other.upFlow;
        this.downFlow += other.downFlow;
        setSumFlow();
    }
}
