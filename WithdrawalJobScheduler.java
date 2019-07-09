package wg.itf.job;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.hand.hap.core.impl.RequestHelper;
import com.hand.hap.job.AbstractJob;

import wg.am.dto.WithdrawalApply;
import wg.am.service.IWithdrawalApplyService;
import wg.fnd.lock.DistributedLockConstants;
import wg.fnd.lock.DistributedLockHelper;
import wg.itf.adapter.impl.sap.CustBalanceWithdrawAdapter;


public class WithdrawalJobScheduler extends AbstractJob {
	
	@Autowired
	private IWithdrawalApplyService withdrawalApplyService;
	@Autowired
	private CustBalanceWithdrawAdapter custBalanceWithdrawAdapter;
	/**
	 * redis锁
	 */
	@Autowired
	protected DistributedLockHelper redisLock;
	
	private final Logger logger = LoggerFactory.getLogger(WithdrawalJobScheduler.class);

	/**
	 * 错误信息：redis锁错误：获取锁失败
	 */
	private static final String GET_REDIS_LOCK_FALSE = "Service get redis lock false!";
	
	@Override
	public void safeExecute(JobExecutionContext context) throws Exception {
		//获取锁
		final String redisLockKey = DistributedLockConstants.WITHDRAW_JOB_KEY+this.getClass().getName();
		if(this.redisLock == null){
			throw new Exception(GET_REDIS_LOCK_FALSE);
		}
		if(!redisLock.lock(redisLockKey)){
			throw new Exception(DistributedLockConstants.RESOURCE_BUSY_NOW);
		}
		try{
			List<WithdrawalApply> applyList = withdrawalApplyService.selectWithdrawalInterfaceData();
			if(CollectionUtils.isEmpty(applyList)) return;
			for(WithdrawalApply apply : applyList){
				try{
					custBalanceWithdrawAdapter.executeInterface(apply);
					//成功后回写Y到WithdrawalApply的同步标识
					WithdrawalApply applyForUpdate = new WithdrawalApply();
					applyForUpdate.setApplyId(apply.getApplyId());
					applyForUpdate.setSyncFlag("Y");
					// 更新提现申请
					this.withdrawalApplyService.updateWithdrawalApply(RequestHelper.newEmptyRequest(), applyForUpdate);
				}catch (Exception ex){
					logger.error("",ex);
					//失败后回写E到WithdrawalApply的同步标识
					WithdrawalApply applyForUpdate = new WithdrawalApply();
					applyForUpdate.setApplyId(apply.getApplyId());
					applyForUpdate.setSyncFlag("E");
					// 更新提现申请
					this.withdrawalApplyService.updateWithdrawalApply(RequestHelper.newEmptyRequest(), applyForUpdate);
				}
			}
		}catch (Exception e) {
			//释放锁
			this.redisLock.releaseLock(redisLockKey);
			throw e;
		}
		//释放锁
		this.redisLock.releaseLock(redisLockKey);
	}
}