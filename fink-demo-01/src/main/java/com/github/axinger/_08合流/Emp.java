package com.github.axinger._08合流;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 员工
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Emp {
    // 部门编号
    Integer empNo;

    String empName;

    // 员工所属部门
    Integer deptNo;


    Long ts;
}
