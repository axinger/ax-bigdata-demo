package com.github.axinger.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PulsarTenantCreator {

    public static void main(String[] args) {
        // Pulsar Admin 服务的 URL
        String serviceHttpUrl = "http://hadoop203:18080"; // 替换为你的 Pulsar Admin URL

        // 创建 PulsarAdmin 客户端
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl).build()) {
            // 租户名称
            String tenantName = "axinger";

            // 创建租户
            createTenant(admin, tenantName);

            System.out.println("租户 '" + tenantName + "' 创建成功！");
        } catch (PulsarAdminException | PulsarClientException e) {
            System.err.println("创建租户失败: " + e.getMessage());
        }
    }

    private static void createTenant(PulsarAdmin admin, String tenantName) throws PulsarAdminException {
        // 获取 Tenants 接口
        Tenants tenants = admin.tenants();

        // 检查租户是否已存在
        if (tenants.getTenants().contains(tenantName)) {
            System.out.println("租户 '" + tenantName + "' 已存在，跳过创建。");
            return;
        }

        // 配置租户信息
        Set<String> allowedClusters = new HashSet<>();
        allowedClusters.add("standalone"); // 允许的集群名称，根据你的集群配置修改

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(allowedClusters)
                .adminRoles(Collections.emptySet()) // 设置管理员角色（可选）
                .build();

        // 创建租户
        tenants.createTenant(tenantName, tenantInfo);
    }
}
