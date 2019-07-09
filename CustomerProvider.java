package wg.itf.provider.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.hand.hap.cache.Cache;
import com.hand.hap.cache.CacheManager;
import com.hand.hap.cache.impl.SysCodeCache;
import com.hand.hap.system.dto.Code;
import com.hand.hap.system.dto.CodeValue;

import wg.fnd.dto.AlertMessage;
import wg.fnd.lock.DistributedLockConstants;
import wg.fnd.service.ISequenceService;
import wg.fnd.utils.alert.AlertMessageEnum;
import wg.fnd.utils.alert.AlertMessageProducer;
import wg.itf.bo.inbound.TradeCustomer;
import wg.itf.provider.AbstractWebServiceProvider;
import wg.md.dto.Customer;
import wg.md.dto.TradeCompany;
import wg.md.dto.TradeParty;
import wg.md.dto.TradePartyAttach;
import wg.md.mapper.CustomerMapper;
import wg.md.mapper.TradePartyMapper;
import wg.md.service.ICustomerService;
import wg.md.service.ITradeCompanyService;
import wg.md.service.ITradePartyAttachService;
import wg.md.service.ITradePartyService;
import wg.tm.dto.CompanyCertify;
import wg.tm.service.ICompanyCertifyService;
import wg.um.dto.UmRole;
import wg.um.dto.UmUser;
import wg.um.dto.UmUserRole;
import wg.um.dto.UserContact;
import wg.um.mapper.UmRoleMapper;
import wg.um.mapper.UmUserMapper;
import wg.um.service.IUmUserRoleService;
import wg.um.service.IUmUserService;
import wg.um.service.IUserContactService;


@Component
public class CustomerProvider extends AbstractWebServiceProvider<TradeCustomer> {
	@Autowired
	private CustomerMapper customerMapper;
	@Autowired
	private TradePartyMapper tradePartyMapper;
	@Autowired
	private ICustomerService customerService;
	@Autowired
	private ITradePartyService tradePartyService;
	@Autowired
	private ITradeCompanyService companyService;
	@Autowired
	private ISequenceService sequenceService;
	@Autowired
	private ICompanyCertifyService companyCertifyService;
	@Autowired
    private CacheManager cacheManager;
	@Autowired
	private UmUserMapper userMapper;
	@Autowired
	private IUmUserService userService;
	@Autowired
	private IUserContactService userContactService;
	@Autowired
	private IUmUserRoleService iUmUserRoleService;
	@Autowired
	private UmRoleMapper umRoleMapper;
	@Autowired
	private ITradePartyAttachService tradePartyAttachService;
	@Autowired
	private AlertMessageProducer messageSender;
	
	/**
	 * 消息：客户编码信息错误
	 */
	private static final String CUSTOMER_NUM_ERROR = "客户编码信息错误";
	/**
	 * 消息：该增值税号已被另一个客户编码使用
	 */
	private static final String TAX_NUM_USED = "该增值税号已被另一个客户编码使用";
	/**
	 * 编码：中国
	 */
	private static final String CHINA = "中国";
	/**
	 * 编码：国内
	 */
	private static final String DOMESTIC = "国内";
	
	/**
	 * 默认前台用户角色代码
	 */
	private static final String DEFAULT_USER_ROLE = "CUSTOMER";
	/**
	 * 企业认证审批状态：已通过
	 */
	private static final String CERTIFY_STATUS_CODE_CPL = "COMPLETE";
	/**
	 * 企业认证审批状态：处理中
	 */
	private static final String CERTIFY_STATUS_CODE_PRO = "PROCESS";
	
	/**
	 * 快码缓存器
	 */
	private Cache<?> cache = null;
	
	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = {Exception.class})
	public TradeCustomer processer(TradeCustomer bo) throws Exception {
		//获取锁
		final String redisLockKey = DistributedLockConstants.ITF_CUSTOMER_KEY + bo.getCustomerNumber();
		if(this.redisLock == null){
			throw new Exception(AbstractWebServiceProvider.GET_REDIS_LOCK_FALSE);
		}
		if(!redisLock.lock(redisLockKey)){
			throw new Exception(AbstractWebServiceProvider.RESOURCE_BUSY_NOW);
		}
		try{
			Date now = new Date();
			//如果快码缓存器为空则初始化
			if(this.cache == null){
				this.cache = cacheManager.getCache("code");
			}
			//根据供应商编号和主体编号去数据库中查有没有
			Customer customerForQuery = new Customer();
			customerForQuery.setCustomerNumber(bo.getCustomerNumber());
			List<Customer> customerQueryResult = customerMapper.select(customerForQuery);
			
			TradeParty tradePartyForQuery = new TradeParty();
			tradePartyForQuery.setTaxNumber(bo.getTaxNumber());
			List<TradeParty> TradePartyQueryResult = tradePartyMapper.select(tradePartyForQuery);
			
			int custCount = CollectionUtils.isEmpty(customerQueryResult)?0:1;
			int partyCount = CollectionUtils.isEmpty(TradePartyQueryResult)?0:1;
			
			/**
			 * 是否新建了供应商，消息提醒用
			 */
			boolean insertCustomer = false;
			
			//分支处理
			if(partyCount == 0){
				if(custCount == 0){
					//主体编码不存在，客户编码也不存在，新建一条主体信息、客户信息；
					TradeParty tradeParty = this.insertTradeParty(bo);
					this.insertCustomer(bo, tradeParty.getTradePartyId(), now);
					insertCustomer = true;
				}else{
					//主体编码不存在，客户编码存在，报错，报错信息：“xxx（客户编码）信息错误”；
					throw new Exception(CustomerProvider.CUSTOMER_NUM_ERROR+bo.getCustomerNumber());
				}
			}else{
				if(custCount == 0){
					//主体编码存在，客户编码不存在，更新主体信息,新建一条客户信息；
					TradeParty aim = TradePartyQueryResult.get(0);
					aim = this.updateTradeParty(aim, bo);
					this.insertCustomer(bo, aim.getTradePartyId(), now);
					insertCustomer = true;
				}else{
					//主体编码存在，客户编码存在，更新主体信息、客户信息；
					TradeParty aimParty = TradePartyQueryResult.get(0);
					Customer aimCust = customerQueryResult.get(0);
					aimParty = this.updateTradeParty(aimParty, bo);
					this.updateCustomer(aimCust, bo, aimParty.getTradePartyId(), now);
				}
			}
			
			/**
			 * 消息提醒
			 */
			if(insertCustomer) {
				//新增供应商
				AlertMessage message = new AlertMessage();
				message.setIndexId(bo.getCustomerNumber());
				message.setAlertMessageCode(AlertMessageEnum.XZKH);
				this.messageSender.sendMessage(message);
			}
		}catch (Exception e) {
			//释放锁
			this.redisLock.releaseLock(redisLockKey);
			throw e;
		}
		//释放锁
		this.redisLock.releaseLock(redisLockKey);
		return null;
	}
	
	/**
	 * 插入客户
	 * @param bo bo
	 * @param tradePartyId 交易主体ID
	 * @param now 当前日期
	 * @throws Exception 
	 */
	private void insertCustomer(TradeCustomer bo, Long tradePartyId, Date now) throws Exception{
		Customer customer = this.getCustomerFromBo(new Customer(), bo, now, true);
		customer.setTradePartyId(tradePartyId);
		
		if(this.customerMapper.selectCountOnTradePartyId(customer) > 0){
			throw new Exception(CustomerProvider.TAX_NUM_USED);
		}
		this.customerService.insertSelective(this.defaultIRequest, customer);
	}
	
	/**
	 * 更新客户
	 * 
	 * @param aim 数据库中查出来的客户
	 * @param bo bo
	 * @param tradePartyId 交易主体ID
	 * @param now 当前日期
	 */
	private void updateCustomer(Customer aim, TradeCustomer bo, Long tradePartyId, Date now) throws Exception{
		aim = this.getCustomerFromBo(aim, bo, now, false);
		aim.setTradePartyId(tradePartyId);
		if(this.customerMapper.selectCountOnTradePartyId(aim) > 0){
			throw new Exception(CustomerProvider.TAX_NUM_USED);
		}
		
		this.customerService.updateByPrimaryKey(this.defaultIRequest, aim);
	}
	
	/**
	 * 插入交易主体
	 * @param bo bo
	 * @return
	 * @throws Exception 
	 */
	private TradeParty insertTradeParty(TradeCustomer bo) throws Exception{
		TradeParty tradeParty = new TradeParty();
		tradeParty.setPartyName(bo.getPartyName());
		tradeParty.setPartyNumber(this.sequenceService.getSequence("TRADE_PARTY_NUMBER"));
		
		TradeCompany company = new TradeCompany();
		company.setCompanyName(tradeParty.getPartyName());
		company = this.insertCompany(company);
		tradeParty.setTradeCompanyId(company.getTradeCompanyId());
		
		tradeParty = this.getTradePartyFromBo(tradeParty, bo);
		
		//根据增值税号查找企业认证信息
		CompanyCertify certify = this.getCompanyCertify(bo.getTaxNumber());
		if(certify == null){
			//如果没查到，直接写入系统
			tradeParty.setLicenseNumber(null);
		}else{
			//查到了，则要将该认证信息设置为已审核
			certify.setApproveStatusCode(CERTIFY_STATUS_CODE_CPL);
			//将该认证信息公司ID设置为this的公司ID
			certify.setTradeCompanyId(tradeParty.getTradeCompanyId());
			companyCertifyService.updateCompanyCertify(certify);
			tradeParty.setLicenseNumber(certify.getLicenseNumber());
		}
		tradeParty = this.tradePartyService.insertSelective(this.defaultIRequest, tradeParty);
		if(certify != null){
			this.processUmUser(certify, tradeParty);
			tradeParty = this.insertAttachmentFromCertify(tradeParty, certify);
		}
		
		return tradeParty;
	}
	
	/**
	 * 将认证信息中的附件Copy到交易主体下
	 * @param aim
	 * @param certify
	 */
	private TradeParty insertAttachmentFromCertify(TradeParty aim, CompanyCertify certify){
		List<TradePartyAttach> attachs = new ArrayList<TradePartyAttach>();
		Long tradePartyId = aim.getTradePartyId();
		TradePartyAttach attach;
		//营业执照
		if(certify.getBusLicenceAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getBusLicenceAttchId());
			attach.setAttachName("营业执照");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//组织机构代码证
		if(certify.getOrganizationAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getOrganizationAttchId());
			attach.setAttachName("组织机构代码证");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//税务登记证
		if(certify.getTaxAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getTaxAttchId());
			attach.setAttachName("税务登记证");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//开户许可证
		if(certify.getAccountAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getAccountAttchId());
			attach.setAttachName("开户许可证");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//开票资料
		if(certify.getBillingAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getBillingAttchId());
			attach.setAttachName("开票资料");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//企业认证授权书
		if(certify.getOrgAuthAttchId() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getOrgAuthAttchId());
			attach.setAttachName("企业认证授权书");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		//提现账户备案
		if(certify.getEnchashmentAcctRecord() != null) {
			attach = new TradePartyAttach();
			attach.setTradePartyId(tradePartyId);
			attach.setAttachObjectId(certify.getEnchashmentAcctRecord());
			attach.setAttachName("提现账户备案");
			attach.setAttribute1(String.valueOf(certify.getCertifyId()));
			attachs.add(attach);
		}
		
		attachs.stream().forEach(atta -> {
			atta = this.tradePartyAttachService.insertSelective(this.defaultIRequest, atta);
		});
		
		aim.setTradePartyAttachs(attachs);
		return aim;
	}
	
	/**
	 * 更新交易主体
	 * @param aim 数据库中查出来的交易主体
	 * @param bo bo
	 * @return
	 * @throws Exception 
	 */
	private TradeParty updateTradeParty(TradeParty aim, TradeCustomer bo) throws Exception{
		aim.setPartyName(bo.getPartyName());
		/**
		 * 更新交易主体时放弃更新公司
		 * 17-06-20
		 */
//		TradeCompany company = new TradeCompany();
//		company.setCompanyNumber(aim.getPartyNumber());
//		company.setCompanyName(aim.getPartyName());
//		company = this.updateOrInsertCompany(company);
//		aim.setTradeCompanyId(company.getTradeCompanyId());
		
		aim = this.getTradePartyFromBo(aim, bo);
		//根据增值税号查找企业认证信息
		CompanyCertify certify = this.getCompanyCertify(bo.getTaxNumber());
		if(certify != null && CERTIFY_STATUS_CODE_PRO.equals(certify.getApproveStatusCode())){
			//查到了，则要将该认证信息设置为已审核
			certify.setApproveStatusCode(CERTIFY_STATUS_CODE_CPL);
			//将该认证信息公司ID设置为this的公司ID
			certify.setTradeCompanyId(aim.getTradeCompanyId());
			companyCertifyService.updateCompanyCertify(certify);
			aim.setLicenseNumber(certify.getLicenseNumber());
			
			aim = this.deleteOldAttachment(aim, certify);
			aim = this.insertAttachmentFromCertify(aim, certify);
		}
		aim = this.tradePartyService.updateTradePartyInfo(this.defaultIRequest, aim);
		
		return aim;
	}
	
	/**
	 * 插入公司
	 * @param company 填充了公司名称的对象
	 * @return
	 */
	private TradeCompany insertCompany(TradeCompany company){
		company.setCompanyNumber(this.sequenceService.getSequence("TRADE_COMPANY"));
		company.setEnabledFlag("Y");
		company = this.companyService.insertSelective(this.defaultIRequest, company);
		return company;
	}
	
	/**
	 * 从bo中获得交易主体信息
	 * @param aim 复制到这个对象
	 * @param bo bo
	 * @return
	 * @throws Exception 
	 */
	private TradeParty getTradePartyFromBo(TradeParty aim, TradeCustomer bo) throws Exception{
		aim.setPartyShortName(bo.getPartyShortName());
		aim.setAddress(bo.getAddress());
		aim.setProvince(bo.getProvince());
		aim.setCity(bo.getCity());
		aim.setTelephone(bo.getTelephone());
		aim.setFax(bo.getFax());
		aim.setTaxNumber(bo.getTaxNumber());
		if(StringUtils.isEmpty(aim.getEnableFlag())) {
			aim.setEnableFlag("Y");
		}
		
		if(CustomerProvider.CHINA.equals(bo.getCountryTypeCode()) || CustomerProvider.DOMESTIC.equals(bo.getCountryTypeCode())){
			aim.setCountryTypeCode("INTER");
		}else{
			aim.setCountryTypeCode("EXTER");
		}
		
		return aim;
	}
	
	/**
	 * 从bo中获得客户信息
	 * @param aim 复制到这个对象
	 * @param bo bo
	 * @param now 当前日期
	 * @param insertMode 是否为插入模式
	 * @return
	 */
	private Customer getCustomerFromBo(Customer aim, TradeCustomer bo, Date now, boolean insertMode){
		aim.setCustomerNumber(bo.getCustomerNumber());
		aim.setCustomerTypeCode(StringUtils.isEmpty(bo.getCustomerTypeCode())?"Y001":bo.getCustomerTypeCode());
		aim.setCurrencyCode(StringUtils.isEmpty(bo.getCurrencyCode())?"CNY":bo.getCurrencyCode());
		aim.setSettlementTypeCode(this.getSettlementTypeCode(bo.getSettlementTypeCode()));

		// 新建客户的时候，指定默认值
		if(insertMode) {
			aim.setRecMethodCode("DELIVERY");
			aim.setTradeModeCode("INTER");
			aim.setContractStartDate(now);
			aim.setEnabledFlag("Y");
		}
		return aim;
	}
	
	/**
	 * 根据SAP传来的增值税号查找系统中的企业认证信息
	 * @param taxNumber
	 * @return 系统中的企业认证信息<b>[可能为空]</b>
	 */
	private CompanyCertify getCompanyCertify(String taxNumber){
		CompanyCertify certifyForQuery = new CompanyCertify();
		certifyForQuery.setVatNumber(taxNumber);
		CompanyCertify certify = companyCertifyService.selectCompanyCertify(certifyForQuery);
		return certify;
	}
	
	/**
	 * 根据SAP回传的结算类型TAG，查找对应的快码值
	 * @param tag tag值
	 * @return 快码值
	 */
	public String getSettlementTypeCode(String tag){
		if(this.cache == null || !(this.cache instanceof SysCodeCache)){
			this.cache = cacheManager.getCache("code");
		}
		if (!(this.cache instanceof SysCodeCache)) {
			return null;
		}
		Code code = ((SysCodeCache) cache).getValue("MD.SETTLEMENT_TYPE" + "." + "zh_CN");
		List<CodeValue> values = code.getCodeValues();
		String res = tag;
		for(CodeValue value : values){
			if(tag.equals(value.getTag())){
				res = value.getValue();
				break;
			}
		}
		return res;
	}
	
	/**
	 * 新建交易主体时如果该主体走了企业认证则将其主体ID挂到前台用户ID上
	 * @param certify
	 * @param tradeParty
	 */
	private void processUmUser(CompanyCertify certify, TradeParty tradeParty){
		//准备数据
		Long umUserId = certify.getFuserId();
		Long companyId = tradeParty.getTradeCompanyId();
		String purchaserName = certify.getPurchaserName();
		Long tradePartyId = tradeParty.getTradePartyId();
		String idNumber = certify.getIdNumber();
		//根据企业认证中的fuserID查找前台用户表中的用户，将新建的的公司ID赋值过去
		UmUser userForQuery = new UmUser();
		userForQuery.setUserId(umUserId);
		UmUser user = this.userMapper.selectOne(userForQuery);
		if(user == null) return;
		user.setTradeCompanyId(companyId);
		user.setFullName(purchaserName);
		user.setIdNumber(idNumber);
		user = this.userService.updateUmUserSelective(this.defaultIRequest, user);
		
		//插入一条前台用户联系信息
		UserContact contact = new UserContact();
		contact.setUserId(user.getUserId());
		contact.setTradePartyId(tradePartyId);
		contact = this.userContactService.insertSelective(this.defaultIRequest, contact);
		
		//刷新用户联系信息Redis缓存
		this.userService.refreshUserContactRedis(contact);
		
		//新增默认前台用户角色信息 xianzhi.chen@hand-china.com
		// 获取默认角色ID
		UmRole umRole = new UmRole();
		umRole.setRoleCode(DEFAULT_USER_ROLE);
		umRole = umRoleMapper.selectOne(umRole);
		// 构建用户角色关系对象，新增默认前台用户角色
		if(umRole.getRoleId() != null){
			UmUserRole umUserRole = new UmUserRole();
			umUserRole.setUserId(umUserId);
			umUserRole.setRoleId(umRole.getRoleId());
			iUmUserRoleService.insertUserRole(this.defaultIRequest, umUserRole);
		}
	}
	
	/**
	 * 从交易主体附件表中删除某企业认证带过去的附件<br/>
	 * 在更新企业认证时使用
	 * @param aim 目标交易主体
	 * @param certify 企业认证DTO
	 * @return
	 */
	private TradeParty deleteOldAttachment(TradeParty aim, CompanyCertify certify) {
		this.tradePartyAttachService.deleteAttachByCertifyId(this.defaultIRequest, certify.getCertifyId());
		aim.setTradePartyAttachs(null);
		return aim;
	}
}