package com.jcandia.course.springcloud.kafka.command.handlers;

import com.jcandia.course.springcloud.kafka.command.models.Command;
import com.jcandia.course.springcloud.kafka.command.models.CommandType;
import com.jcandia.course.springcloud.kafka.command.models.Reply;
import com.jcandia.course.springcloud.kafka.command.models.ReplyStatus;
import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;
import com.jcandia.course.springcloud.kafka.command.services.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

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
            Reply<?> reply;
            switch (cmd.type()) {
                case CommandType.CREATE -> {
                    if( cmd.body() == null ) {
                        log.warn("[CREATE] Empty body");
                        reply = new Reply<>(ReplyStatus.ERROR, "Empty body", null);
                    }
                    ProductDTO savedProduct = service.create(cmd.body());

                    log.info("Creating product: name={}, price={}", savedProduct.name(), savedProduct.price());
                    reply = new Reply<>(ReplyStatus.SUCCESS, "Product created", savedProduct);
                }
                case CommandType.READ -> {
                    if( cmd.id() == null ) {
                        log.warn("Read empty ID");
                        reply = new Reply<>(ReplyStatus.ERROR, "ID is required", null);
                    }

                    ProductDTO productDTO = service.findById(cmd.id());

                    reply = (productDTO == null) ?
                        new Reply<>(ReplyStatus.ERROR, "Product not found", null) :
                        new Reply<>(ReplyStatus.SUCCESS, "Read product name", productDTO);

                    log.info("Finding product: id={}", cmd.id());
                }
                case CommandType.READ_ALL -> {
                    reply = new Reply<>(ReplyStatus.SUCCESS, "Read all products", service.findAll());
                    log.info("Getting all products");
                }
                case CommandType.UPDATE -> {
                    if( cmd.body() == null || cmd.id() == null ) {
                        log.warn("ID and body are required");
                        reply = new Reply<>(ReplyStatus.ERROR, "ID and body are required", null);
                    }

                    ProductDTO productDTO = service.findById(cmd.id());

                    if( productDTO != null ){
                        reply = new Reply<>(ReplyStatus.SUCCESS, "Update product name", productDTO);
                        log.info("Updating product: name={}, price={}", productDTO.name(), productDTO.price());
                    } else {
                        reply = new Reply<>(ReplyStatus.ERROR, "Product not found", null);
                        log.warn("Product not found, null Product DTO");
                    }

                }
                case CommandType.DELETE -> {
                    if( cmd.id() == null ) {
                        log.warn("ID is required");
                        reply = new Reply<>(ReplyStatus.ERROR, "ID is required", null);
                    }

                    boolean result = service.delete(cmd.id());
                    reply = (result) ?
                            new Reply<>(ReplyStatus.SUCCESS, "Deleting product", "deleted") :
                            new Reply<>(ReplyStatus.ERROR, "Product not found", null);

                    log.info("Deleting product: id={}", cmd.body().id());
                }
                default -> {
                    log.warn("Unknown command type={}", cmd.type());
                    reply = new Reply<>(ReplyStatus.ERROR, "Unknown command type", service.findAll());
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
