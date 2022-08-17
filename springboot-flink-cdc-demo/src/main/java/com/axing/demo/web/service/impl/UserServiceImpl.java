package com.axing.demo.web.service.impl;

import com.axing.demo.web.service.UserService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.axing.demo.web.domain.User;
import com.axing.demo.web.mapper.UserMapper;
import org.springframework.stereotype.Service;

/**
* @author xing
* @description 针对表【user】的数据库操作Service实现
* @createDate 2022-08-17 09:22:28
*/
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User>
    implements UserService {

}




