package com.github.axinger.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;

@Slf4j
public class PulsarTenantCreateNamespace {
    public static void main(String[] args) throws PulsarAdminException, PulsarClientException {
        // 初始化PulsarAdmin客户端
        String serviceHttpUrl = "http://hadoop203:18080"; // 替换为你的 Pulsar Admin URL
        PulsarAdminBuilder builder = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl); // 替换成你的Pulsar服务HTTP URL
        PulsarAdmin admin = builder.build();

        // 定义租户和命名空间名称
        String tenantNew = "axinger"; // 替换成你的租户名称
        String namespace = "demo1"; // 替换成你想要创建的命名空间名称

        // 创建命名空间
        admin.namespaces().createNamespace(tenantNew + "/" + namespace);

        // 查询所有租户
        List<String> tenants = admin.tenants().getTenants();
        // 打印租户列表
        System.out.println("已有的租户列表=" + tenants);
        for (String tenant : tenants) {
            // 列出指定租户的所有命名空间
            List<String> namespaces = admin.namespaces().getNamespaces(tenant);
            log.info("tenant={},namespaces={} ", tenant, namespaces);
        }

    }
}
