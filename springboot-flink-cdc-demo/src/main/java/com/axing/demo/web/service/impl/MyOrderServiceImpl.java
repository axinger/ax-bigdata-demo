package com.axing.demo.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.axing.demo.web.domain.MyOrder;
import com.axing.demo.web.service.MyOrderService;
import com.axing.demo.web.mapper.MyOrderMapper;
import org.springframework.stereotype.Service;

/**
* @author xing
* @description 针对表【my_order】的数据库操作Service实现
* @createDate 2022-08-17 12:56:06
*/
@Service
public class MyOrderServiceImpl extends ServiceImpl<MyOrderMapper, MyOrder>
    implements MyOrderService{

}




