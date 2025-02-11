package com.github.axinger.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.List;

@Slf4j
public class PulsarTenantLister {

    public static void main(String[] args) {
        // Pulsar Admin 服务的 URL
        String serviceHttpUrl = "http://hadoop203:18080"; // 替换为你的 Pulsar Admin URL

        // 创建 PulsarAdmin 客户端
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl).build()) {
            // 查询所有租户
            List<String> tenants = admin.tenants().getTenants();


            // 打印租户列表
            System.out.println("已有的租户列表=" + tenants);


            for (String tenant : tenants) {

                // 列出指定租户的所有命名空间
                List<String> namespaces = admin.namespaces().getNamespaces(tenant);
                TenantInfo tenantInfo = admin.tenants().getTenantInfo(tenant);

                log.info("tenant={},namespaces={},tenantInfo={} ", tenant, namespaces, tenantInfo);

                for (String namespace : namespaces) {
                    for (String topic : admin.namespaces().getTopics(namespace)) {
                        log.info("查询主题: tenant={},namespaces={},topic={} ", tenant, namespace, topic);
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("查询租户失败: " + e.getMessage());
        }
    }

}
