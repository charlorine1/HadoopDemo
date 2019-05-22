package com.usst.spark.other.accumulator;

import org.apache.spark.AccumulatorParam;




// 自定义accumulotor 累加器

/**
 * 
		 假设样本数据集合为simple={1,2,3,4}
		
		执行顺序:
		
		1.调用zero(initialValue)，返回zeroValue
		
		2.调用addAccumulator(zeroValue,1) 返回v1.
		
		调用addAccumulator(v1,2)返回v2.
		
		调用addAccumulator(v2,3)返回v3.
		
		调用addAccumulator(v3,4)返回v4.
		
		3.调用addInPlace(initialValue,v4)
		
		因此最终结果是zeroValue+1+2+3+4+initialValue.
    * */
public class LongAccumulator implements AccumulatorParam<Long> {
	
	private static final long serialVersionUID = 1L;

    /*
     * init 就是SparkContext.accumulator(init)参数init。
          * 这里的返回值是累计的起始值。注意哦，他可以不等于init。
     *
          * 如果init=10,zero(init)=0,那么运算过程如下:
     * v1:=0+step
     * v1:=v1+step
     * ...
     * ...
         * 最后v1:=v1+init
     **/
	@Override
	public Long zero(Long init) {
		System.out.println("zero 方法的init = " + init);
		return 0L;
	}

	/**
	 * init 是 sc.accumulator(20L,new LongAccumulator()); 传入的值
	 * value 是所有task 遍历后 add（）执行后， 经过addInPlace方法 ，得到的 值
	 * 
	 * */
	@Override
	public Long addInPlace(Long init, Long value) {
        // TODO Auto-generated method stub
        // return arg0+arg1;
        System.out.println(init+":"+value);
        return init+value;
	}

	
	/**
	 * 
	 * 第一次：value = 0 ，step 是第一次传入的值
	 *第二次开始： value 是前一次该方法执行后的值，step是 task传入的值
	 * 
	 * */
	@Override
	public Long addAccumulator(Long value, Long step) {
		/*
		 * System.out.println("addaccumulator 方法的参数 value = " + value);
		 * System.out.println("addaccumulator 方法的参数 step = " + step); 
		 * return value+step;
		 */
		return add(value,step);
	}
	
	

	public Long add(Long value, Long step) {
		System.out.println("addaccumulator 方法的参数 value = " + value);
		System.out.println("addaccumulator 方法的参数 step = " + step);
		return value + step;
	}

}
