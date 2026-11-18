package com.jpmc.midascore.foundation;

import java.math.BigDecimal;

/**
 * 金额相关的统一常量：精度（小数位数）与零值。
 *
 * <p>业务侧判定「金额是否合法」、实体映射、数据库列都必须用同一个精度，
 * 分散定义迟早会出现一边 2 位、一边 4 位的漂移。
 */
public final class Money {

    /** 金额小数位数，与 NUMERIC(19,4) 及实体上的 scale 保持一致 */
    public static final int SCALE = 4;

    /** 精度对齐后的零值，避免 0 与 0.0000 混用 */
    public static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

    private Money() {
    }
}
