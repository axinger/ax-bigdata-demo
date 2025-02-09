package com.github.axinger._13sql;

import org.apache.flink.table.factories.Factory;

import java.util.ServiceLoader;

///  检查 Flink Pulsar 连接器是否加载
public class CheckFlinkFactories {
    public static void main(String[] args) {
        ServiceLoader<Factory> factories = ServiceLoader.load(Factory.class);
        for (Factory factory : factories) {
            System.out.println("Available factory: " + factory.factoryIdentifier());
        }
    }
}
