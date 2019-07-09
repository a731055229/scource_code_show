package wg.fnd.utils.db;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * 数据源切换AOP切面指定注解拦截处理类
 */
@Aspect
@Component
public class DynamicDataSourceAspect {
	
	@Around("@annotation(wg.fnd.utils.db.ReadDataSource)")
	public Object around(ProceedingJoinPoint pjp) throws Throwable {
		DynamicDataSourceHolder.setDataSourceType(DynamicDataSourceHolder.DATA_SOURCE_RO);
		Object object = pjp.proceed();
		DynamicDataSourceHolder.clearDataSourceType();
		return object;
	}

}
