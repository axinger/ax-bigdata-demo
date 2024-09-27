package com.github.axinger._11ReducerJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //创建集合

        // 存放订单的
        List<TableBean> orderBeanList = new ArrayList<>();

        //存放产品信息的
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
//                orderBeanList.add(value); // 这里hadoop,会覆盖,只有最后一个,所以需要copy对象

                TableBean tempBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tempBean, value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                orderBeanList.add(tempBean);
            } else {
                //商品表
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //循环遍历,取pName
        for (TableBean bean : orderBeanList) {
            // 聚合后, pid 相同,所以不需要判断
            bean.setPdName(pdBean.getPdName());
            //写出
            context.write(bean, NullWritable.get());
        }
    }
}
