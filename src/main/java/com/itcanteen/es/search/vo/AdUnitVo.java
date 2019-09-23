package com.itcanteen.es.search.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/19 10:32
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdUnitVo {

    private Long id;
    private Long plan_id;
    private String  unit_name;
    private Integer unit_status;
    private Integer position_type;
    private Long budget;


}
