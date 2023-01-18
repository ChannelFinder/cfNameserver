package org.phoebus.channelfinder.nameserver;

import org.epics.pva.PVASettings;
import org.epics.pva.common.AddressInfo;
import org.epics.pva.common.Network;
import org.epics.pva.common.SearchRequest;
import org.epics.pva.data.Hexdump;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

import static org.epics.pva.common.SearchRequest.Channel;
import static org.phoebus.channelfinder.nameserver.CFNameserver.logger;

public class CFSearchRequestRelay {

    private final DatagramChannel udp_search4;
    private final DatagramChannel udp_search6;
    private final InetSocketAddress udp_localaddr4;
    private final InetSocketAddress udp_localaddr6;
    private final List<AddressInfo> broadcast;

    //private final AddressInfo local_multicast;
    private final ByteBuffer forward_buffer = ByteBuffer.allocate(PVASettings.MAX_UDP_PACKET);

    public CFSearchRequestRelay() throws Exception {
        // IPv4 socket, also used to send broadcasts
        udp_search4 = Network.createUDP(StandardProtocolFamily.INET, null, 0);
        udp_search4.socket().setBroadcast(true);
        //local_multicast = Network.configureLocalIPv4Multicast(udp_search4, PVASettings.EPICS_PVA_BROADCAST_PORT);
        broadcast = Network.getBroadcastAddresses(PVASettings.EPICS_PVA_BROADCAST_PORT);

        // IPv6 socket
        udp_search6 = Network.createUDP(StandardProtocolFamily.INET6, null, 0);

        udp_localaddr4 = (InetSocketAddress) udp_search4.getLocalAddress();
        udp_localaddr6 = (InetSocketAddress) udp_search6.getLocalAddress();

        PVASettings.logger.log(Level.FINE, "Awaiting search replies on UDP " + udp_localaddr4 +
                " and " + udp_localaddr6 );
    }

    public void forward(int seq,
                        int cid,
                        String name,
                        InetSocketAddress addr,
                        Consumer<InetSocketAddress> reply_sender) throws Exception {

        logger.info("forwarding search request: " + addr + " searches for " + name + " (seq " + seq + ")");
        synchronized (forward_buffer) {
            Channel channel = new Channel(cid, name);

            for (AddressInfo broadcast_addr : broadcast)
            {
                forward_buffer.clear();
//                final InetSocketAddress response = udp.getResponseAddress(addr);
                SearchRequest.encode(false, seq, List.of(channel), addr, forward_buffer);
                forward_buffer.flip();
                try
                {
                    PVASettings.logger.log(Level.FINER, () -> "Sending search to UDP  " + broadcast_addr + " (broadcast/multicast), " +
                            "response addr " + addr + "\n" + Hexdump.toHexdump(forward_buffer));
                    send(forward_buffer, broadcast_addr);
                }
                catch (Exception ex)
                {
                    PVASettings.logger.log(Level.WARNING, "Failed to send search request to " + addr, ex);
                }
            }
        }
    }

    public void send(final ByteBuffer buffer, final AddressInfo info) throws Exception
    {
        // synchronized (udp_search)?
        // Not necessary based on Javadoc for send(),
        // but in case we set the multicast IF & TTL
        if (info.getAddress().getAddress() instanceof Inet4Address)
        {
            synchronized (udp_search4)
            {
                if (info.getAddress().getAddress().isMulticastAddress())
                {
                    udp_search4.setOption(StandardSocketOptions.IP_MULTICAST_IF, info.getInterface());
                    udp_search4.setOption(StandardSocketOptions.IP_MULTICAST_TTL, info.getTTL());
                }
                PVASettings.logger.log(Level.FINER, () -> "Sending search to UDP  " + info.getAddress() + " (unicast), " +
                        "response addr " + "response" + "\n" + Hexdump.toHexdump(buffer));
                udp_search4.send(buffer, info.getAddress());
            }
        }
        else
        {
            synchronized (udp_search6)
            {
                if (info.getAddress().getAddress().isMulticastAddress())
                {
                    udp_search6.setOption(StandardSocketOptions.IP_MULTICAST_IF, info.getInterface());
                    udp_search6.setOption(StandardSocketOptions.IP_MULTICAST_TTL, info.getTTL());
                }
                udp_search6.send(buffer, info.getAddress());
            }
        }
    }
}
