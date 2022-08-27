package com.axing.demo.web.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import lombok.Data;

/**
 * 
 * @TableName order
 */
@TableName(value ="t_order")
@Data
public class Order implements Serializable {
    /**
     * 
     */
    @TableId
    private Long id;

    /**
     * 订单号
     */
    private String no;

    /**
     * 货物名称
     */
    private String name;

    @TableField(exist = false)
    private static final long serialVersionUID = 1L;
}