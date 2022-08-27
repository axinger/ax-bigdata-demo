package com.axing.demo;

import com.axing.demo.web.domain.Order;
import com.axing.demo.web.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DemoApplicationTests {

    @Autowired
    private OrderService orderService;



    @Test
    void contextLoads() {

        Order order = new Order();
        order.setNo("no-");
        orderService.save(order);

    }
}
