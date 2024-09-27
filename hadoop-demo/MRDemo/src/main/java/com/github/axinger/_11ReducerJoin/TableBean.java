package com.github.axinger._11ReducerJoin;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableBean implements Writable {

    private String id; // 订单id
    private String pdId; // 商品id
    private int amount; // 商品数量
    private String pdName;// 商品名称
    private String flag; // 标记是什么表 order pd


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pdId);
        out.writeInt(amount);
        out.writeUTF(pdName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readUTF();
        this.pdId = in.readUTF();
        this.amount = in.readInt();
        this.pdName = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        // id	pname	amount
        return id + "\t" + pdName + "\t" + amount;
    }
}
