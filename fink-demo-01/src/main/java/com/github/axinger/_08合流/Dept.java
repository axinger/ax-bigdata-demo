package com.github.axinger._08合流;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dept {

    // 部门编号
    Integer deptNo;

    // 部门名称
    String deptName;

    Long ts;
}
