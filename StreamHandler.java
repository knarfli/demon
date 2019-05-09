package com

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import org.apache.log4j.Logger;



public class StreamHandler implements Runnable {

	private String address;

	private static Logger logger = Logger.getLogger(StreamHandler.class);

	public StreamHandler(String address) {
		super();
		this.address = address;
	}

	@Override
	public void run() {
		
		try{
			
			SocketChannel channel = PropHandler.CHANNEL_MAP.get(address);

			if(logger.isDebugEnabled())
			{
				logger.debug("process the input stream");
			}
			int size = 0;
			ByteBuffer buffer = ByteBuffer.allocate(PropHandler.BUFFER_SIZE);
			while (null != channel) {
				if(logger.isDebugEnabled())
				{
					logger.debug("process channel start");
				}
				if (((CommonUtils.isEaAvailable() && null != PropHandler.TRANSFER_QUEUE) || !CommonUtils.isEaAvailable())
						&& null != PropHandler.WRITE_QUEUE) {
					try {
						if(logger.isDebugEnabled())
						{
							logger.debug("the source ip is:" + address);
						}
						

						while ((size = channel.read(buffer)) > 0) {
							if(logger.isDebugEnabled())
							{
								logger.debug("the size of read buffer is" + size);
							}
							
							buffer.flip();
							
							byte[] bytes = new byte[buffer.limit()];
							buffer.get(bytes);
							
							BytesElement bytesElement = CommonUtils.generateBytesElement(address, bytes);

							if (null != bytesElement && CommonUtils.isEaAvailable() && CommonUtils.isEaServerConnectable(address)
									&& !CommonUtils.reachQueueLimit(HandlerUtils.TRANSFER_QUEUE)) {
								BytesElement transferBytesElement = bytesElement.clone();
								PropHandler.TRANSFER_QUEUE.add(transferBytesElement);
								for(EventBean eventBean:transferBytesElement.getEventList())
								{
									if(eventBean.getEventType()==HandlerUtils.HEADER)
									{
										HeaderInfo headerInfo = new HeaderInfo();
										headerInfo.setAddressWithOutPort(CommonUtils.extractIPFromAddress(transferBytesElement.getAddress()));
										headerInfo.setHeader(eventBean.getBytes());
										headerInfo.setSendHeader(false);
										PropHandler.HEADER_INFO_MAP.put(address, headerInfo);
									}
								}
								
							}
							if(null != bytesElement && !CommonUtils.reachQueueLimit(HandlerUtils.WRITE_QUEUE))
							{
								PropHandler.WRITE_QUEUE.add(bytesElement);
								if(logger.isDebugEnabled())
								{
									logger.debug("wrote bytes to queue");
									if(null!=bytesElement)
									{
										logger.debug("the address is:"+bytesElement.getAddress());
										logger.debug("the info is:");
										if(null != bytesElement.getEventList())
										{
											for(EventBean bean:bytesElement.getEventList())
											{
												if(null != bean.getBytes())
												{
													logger.debug("eventBean:[type:"+bean.getEventType()+",length:"+bean.getBytes().length);
												}
												
											}
										}
									}
								}
							}
							
							buffer.clear();

							if(logger.isDebugEnabled())
							{
								logger.debug("wrote bytes to queue");
							}
						}

						
						if (size == -1) 
						{
							CommonUtils.scProcess(address);
							channel = null;
							if(logger.isInfoEnabled())
							{
								logger.info("sapc stoped the connection");
							}
						}
						else
						{
							Thread.sleep(PropHandler.CHANNEL_WAIT_TIME);
						}
						
					} catch (ClosedChannelException e1) {
						logger.error("channel has been closed", e1);
						try {
							CommonUtils.scProcess(address);

						} catch (Exception e) {
							logger.error("close channel failed", e);

						} finally {
							channel = null;
						}

					} catch (Exception e) {
						logger.error("error happened when process stream", e);
						try {
							CommonUtils.scProcess(address);

						} catch (Exception e1) {
							logger.error("error happened when close the channel", e1);

						} finally {
							channel = null;
						}
					}
				}

			}

		}catch(Exception e){
			logger.error("error happened in stream thread",e);
		}

	}

}
