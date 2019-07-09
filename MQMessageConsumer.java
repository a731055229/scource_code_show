package wg.fnd.utils.mns;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.model.Message;

import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;

/**
 * 消息队列消息者线程类
 */
public class MQMessageConsumer implements Callable<String> {
	
	public static final int WAIT_SECONDS = 30;
	
	private MQServiceHelper mQServiceHelper;
	private MNSClient client;
	private String queueName;
	private IMQConsumerService service;
	
	private Logger logger = LoggerFactory.getLogger(MQMessageConsumer.class);

	/**
	 * 消息队列消费服务类构造函数
	 * @param mQServiceHelper MNS服务类
	 * @param queueName 队列名称
	 * @param service 业务逻辑服务
	 */
	public MQMessageConsumer(MQServiceHelper mQServiceHelper,String queueName, IMQConsumerService service) {
		this.mQServiceHelper = mQServiceHelper;
		this.queueName = queueName;
		this.service = service;
	}

	@Override
	public String call() throws Exception {
		if(this.service == null || StringUtils.isBlank(this.queueName)){
			return "Service or Queue Name Is Null!";
		}
		try {
			this.client = this.mQServiceHelper.getMNSClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(this.client == null){
			return "MNS Client Is Null!";
		}
		CloudQueue cloudQueue = this.client.getQueueRef(this.queueName);
		String resultStr = "";
		while (true) {
			Message message = cloudQueue.popMessage(WAIT_SECONDS);
			if (message == null) {
				break;
			}
			if (this.service.consumerProcess(message.getMessageBody())) {
				this.client.getQueueRef(this.queueName).deleteMessage(message.getReceiptHandle());
			}else{
				resultStr = "Queue：" + this.queueName + " Message Consumer Failed!";
				if(logger.isErrorEnabled()){
					logger.error(resultStr);
				}
			}
		}
		if(this.client.isOpen()){
			this.client.close();
		}
		return resultStr;
	}

}
