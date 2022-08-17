package com.axing.demo.web.controller;

import com.axing.demo.web.domain.MyOrder;
import com.axing.demo.web.domain.User;
import com.axing.demo.web.service.MyOrderService;
import com.axing.demo.web.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class IndexController {

    @Autowired
    private MyOrderService orderService;


    @Autowired
    private UserService userService;


    @GetMapping("/add/order")
    public Object addOrder() {

        List<MyOrder> list = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            MyOrder order = new MyOrder();
            order.setNo("no-"+i);
            list.add(order);
        }
        orderService.saveBatch(list);
        return list;
    }


    @GetMapping("/add/user")
    public Object addUser() {

        List<User> list = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            User user = new User();
            user.setName("name-"+i);
            user.setAge(i);
            list.add(user);
        }

        userService.saveBatch(list);
        return list;
    }
}
