package com.jcandia.course.springcloud.kafka.command.handlers;

import com.jcandia.course.springcloud.kafka.command.models.Command;
import com.jcandia.course.springcloud.kafka.command.models.Reply;
import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;
import com.jcandia.course.springcloud.kafka.command.services.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductCommandConsumer.class);

    private final ProductService service;

    public ProductCommandConsumer(ProductService service) {
        this.service = service;
    }

    @Bean
    public Function<Message<Command<ProductDTO>>, Message<Reply<?>>> handleCommands() {
        return msg -> {
            Command<ProductDTO> cmd = msg.getPayload();
            String type = cmd.type() == null ? "" : cmd.type().toUpperCase();
            Reply<ProductDTO> reply;
            switch (type) {
                case "CREATE" -> {
                    if( cmd.body() == null ) {
                        log.warn("[CREATE] Empty body");
                        reply = new Reply<>("ERROR", "Empty body", null);
                    }
                    ProductDTO savedProduct = service.create(cmd.body());

                    log.info("Creating product: name={}, price={}", savedProduct.name(), savedProduct.price());
                    reply = new Reply<>("SUCCESS", "Product created", savedProduct);
                }
                case "FIND" -> {
                    if( cmd.id() == null ) {
                        log.warn("Read empty ID");
                        reply = new Reply<>("ERROR", "ID is required", null);
                    }

                    ProductDTO productDTO = service.findById(cmd.id());

                    reply = (productDTO == null) ?
                        new Reply<>("ERROR", "Product not found", null) :
                        new Reply<>("SUCCESS", "Read product name", productDTO);

                    log.info("Finding product: id={}", cmd.id());
                }
//                case "UPDATE" -> {
//                    log.info("Updating product: name={}, price={}", cmd.body().name(), cmd.body().price());
//                }
//                case "DELETE" -> {
//                    log.info("Deleting product: id={}", cmd.body().id());
//                }
//                case "FIND_ALL" -> {
//                    log.info("Getting all products");
//                }
                default -> {
                    log.warn("Unknown command type={}", cmd.type());
                    reply = new Reply<>("ERROR", "Unknown command type", null);
                }
            }

            String correlationId = msg.getHeaders().get("correlationId", String.class);
            log.info("CorrelationId={}", correlationId);
            MessageBuilder<Reply<?>> out = MessageBuilder.withPayload(reply);

            if( correlationId != null ) {
                out.setHeader("correlationId", correlationId);
            }

            return out.build();
        };
    }

}
