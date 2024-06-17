package org.phoebus.channelfinder.nameserver;

import org.epics.pva.PVASettings;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.SearchHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@SpringBootApplication
public class CFNameserver implements CommandLineRunner {

    @Value("${cf.url:http://localhost:8080}")
    private String cfURL;
    @Value("${cf.resources:/ChannelFinder/resources/channels?}")
    private String cfResource;
    @Value("${cf.property.ioc_ip_address_name:iocIP}")
    private String iocIPPropertyName;
    @Value("${cf.property.pva_port_name:pvaPort}")
    private String pvaPortPropertyName;
    @Value("${cf.timeout:15}")
    private Long timeout;
    @Value("${epics.pva.broadcast.port:5076}")
    private String epicsPVABroadcastPort;
    @Value("${epics.pva.server.port:5076}")
    private String epicsPVAServerPort;
    @Value("${cf.use_pvStatus:false}")
    private Boolean usePVStatus;

    private Duration cfTimeout;
    private String cfQueryString;

    private WebClient client = WebClient.create();

    static Logger logger = Logger.getLogger(CFNameserver.class.getName());

    public static void main(String[] args){
        new SpringApplicationBuilder(CFNameserver.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.setProperty("EPICS_PVAS_BROADCAST_PORT", epicsPVABroadcastPort);
        System.setProperty("EPICS_PVA_SERVER_PORT", epicsPVAServerPort);

        LogManager.getLogManager().readConfiguration(PVASettings.class.getResourceAsStream("/pva_logging.properties"));
        PVASettings.logger.setLevel(Level.ALL);

        cfTimeout = Duration.of(timeout, ChronoUnit.SECONDS);
        if(usePVStatus) {
            cfQueryString = cfURL + cfResource + "pvStatus=Active&~name=";
        }
        else {
            cfQueryString = cfURL + cfResource + "~name=";
        }

        // Start PVA server with custom search handler
        final CountDownLatch done = new CountDownLatch(1);
        final SearchHandler search_handler = (seq, cid, name, addr, reply_sender) ->
        {
            logger.info(addr + " searches for " + name + " (seq " + seq + ")");
            // Quit when receiving search for name "QUIT"
            if (name.equals("QUIT"))
                done.countDown();

            // Check "name server"
            final Optional<InetSocketAddress> server_addr = getAddressforPV(name);
            if (server_addr.isPresent())
            {
                logger.info(" --> Sending client to " + server_addr);
                reply_sender.accept(server_addr.get());
            }

            // Done, don't proceed with default search handler
            return true;
        };

        try(final PVAServer server = new PVAServer(search_handler)) {
            logger.info("For UDP search, run 'pvget' or 'pvxget' with");
            logger.info("EPICS_PVA_BROADCAST_PORT=" + PVASettings.EPICS_PVAS_BROADCAST_PORT);
            logger.info("For TCP search, set EPICS_PVA_NAME_SERVERS = " + server.getTCPAddress(false));
            logger.info("or other IP address of this host and same port.");
            logger.info("Run 'pvget QUIT' to stop");
            done.await();
        }

    }

    /**
     * Using the channel finder property "socket_address" whose value is of the form "ip_address:port" and represents the
     * TCP port of the IOC for setting up connections.
     *
     * @param pvName
     * @return
     */
    private Optional<InetSocketAddress> getAddressforPV(String pvName) {
        // retrieve the channel info from channelfinder
        WebClient.ResponseSpec response = client.get().uri(cfQueryString + pvName).retrieve();
        Mono<XmlChannel> xmlChannelMono = Mono.from(response.bodyToFlux(XmlChannel.class));
        XmlChannel result = xmlChannelMono.block(cfTimeout);
        if(result == null) {
            return Optional.empty();
        }
        // parse the IP address and PVA port properties
        AtomicReference<String> iocIPPropertyValue = new AtomicReference<>();
        result.getProperties().stream()
                .filter(prop -> prop.getName().equalsIgnoreCase(iocIPPropertyName))
                .findFirst().ifPresent(socket -> {
                    System.out.println("found:...");
                    iocIPPropertyValue.set(socket.getValue());
                });
        AtomicReference<String> pvaPortPropertyValue = new AtomicReference<>();
        result.getProperties().stream()
                .filter(prop -> prop.getName().equalsIgnoreCase(pvaPortPropertyName))
                .findFirst().ifPresent(socket -> {
                    System.out.println("found:...");
                    pvaPortPropertyValue.set(socket.getValue());
                });
        if(iocIPPropertyValue.get() != null && pvaPortPropertyValue.get() != null) {
              return Optional.of(new InetSocketAddress(iocIPPropertyValue.get(), Integer.parseInt(pvaPortPropertyValue.get())));
        }
        return Optional.empty();
    }

}
