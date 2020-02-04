package org.littleshoot.proxy.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

public class HttpProxyProtocolRequestDecoder extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(HttpProxyProtocolRequestDecoder.class);
  public static final AttributeKey<String> SOURCE_IP_ATTRIBUTE = AttributeKey.valueOf("sourceIp");

  // Pattern:
  //   PROXY_STRING + single space + INET_PROTOCOL + single space + CLIENT_IP + single space + PROXY_IP + single space + CLIENT_PORT + single space + PROXY_PORT + "\r\n"
  // Example:
  //   PROXY TCP4 198.51.100.22 203.0.113.7 35646 80\r\n
  //   or
  //   PROXY TCP6 2001:DB8::21f:5bff:febf:ce22:8a2e 2001:DB8::12f:8baa:eafc:ce29:6b2e 35646 80
  // Source:
  //   https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/enable-proxy-protocol.html
  private static final Pattern TCP4_PROXY_PROTOCOL_HEADER_PATTERN =
      Pattern.compile("PROXY TCP4 (((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.)?){4}) ((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.)?){4} \\d+ \\d+\\r\\n");

  private static final Pattern TCP6_PROXY_PROTOCOL_HEADER_PATTERN =
      Pattern.compile("PROXY TCP6 (([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}) ([0-9a-f]{1,4}:){7}([0-9a-f]){1,4} \\d+ \\d+\\r\\n", Pattern.CASE_INSENSITIVE);

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf buf = ((ByteBuf) msg);
    try{

      String body = bufToString(buf);

      Matcher tcp4Matcher = TCP4_PROXY_PROTOCOL_HEADER_PATTERN.matcher(body);
      Matcher tcp6Matcher = TCP6_PROXY_PROTOCOL_HEADER_PATTERN.matcher(body);

      String sourceIp;

      if (tcp4Matcher.find()) {
        sourceIp = tcp4Matcher.group(1);
      } else if (tcp6Matcher.find()) {
        sourceIp = tcp6Matcher.group(1);
      } else {
        ctx.fireChannelRead(msg);
        return; // no proxy protocol header found, proceed
      }

      ctx.channel().attr(SOURCE_IP_ATTRIBUTE).set(sourceIp);

      int proxyProtocolHeaderIndex = body.indexOf("\r\n");
      String stripped = body.substring(proxyProtocolHeaderIndex + 2); // +2 for \r\n

      buf.clear().writeBytes(stripped.getBytes());

      ctx.fireChannelRead(buf);
    } catch(Exception ex){
      LOG.error("VMWARE HttpProxyProtocolRequestDecoder error - buf:{}, refCnt:{}. Exception: {}", buf.hashCode(), 
              ReferenceCountUtil.refCnt(buf), ex.getMessage());
        final int refCnt = ReferenceCountUtil.refCnt(msg);
        if(refCnt > 0){
            ReferenceCountUtil.safeRelease(msg, refCnt);
        }
      throw ex;
    }
  }

  private String bufToString(ByteBuf content) {
    final byte[] bytes = new byte[content.readableBytes()];

    content.getBytes(content.readerIndex(), bytes);

    return new String(bytes);
  }
}
