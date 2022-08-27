package com.axing.demo.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.axing.demo.web.domain.Order;
import com.axing.demo.web.service.OrderService;
import com.axing.demo.web.mapper.OrderMapper;
import org.springframework.stereotype.Service;

/**
* @author xing
* @description 针对表【order】的数据库操作Service实现
* @createDate 2022-08-22 11:06:17
*/
@Service
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order>
    implements OrderService{

}




