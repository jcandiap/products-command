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
    public Function<Message<Command<ProductDTO>>, Message<Reply<Object>>> handleCommands() {
        return msg -> {
            String correlationId = msg.getHeaders().get("correlationId", String.class);
            log.info("CorrelationId={}", correlationId);

            if( correlationId == null || correlationId.isBlank() ) {
                return MessageBuilder
                        .withPayload(new Reply<>(ReplyStatus.ERROR, "Missing correlationId", null))
                        .build();
            }

            Command<ProductDTO> cmd = msg.getPayload();
            Reply<Object> reply = switch (cmd.type()) {
                case CommandType.CREATE -> {
                    if( cmd.body() == null ) {
                        log.warn("[CREATE] Empty body");
                        yield new Reply<>(ReplyStatus.ERROR, "Empty body", null);
                    } else {
                        ProductDTO savedProduct = service.create(cmd.body());

                        log.info("Creating product: name={}, price={}", savedProduct.name(), savedProduct.price());
                        yield new Reply<>(ReplyStatus.SUCCESS, "Product created", savedProduct);
                    }
                }
                case CommandType.READ -> {
                    if( cmd.id() == null ) {
                        log.warn("Read empty ID");
                        yield new Reply<>(ReplyStatus.ERROR, "ID is required", null);
                    } else {
                        ProductDTO productDTO = service.findById(cmd.id());

                        log.info("Finding product: id={}", cmd.id());

                        yield (productDTO == null) ?
                            new Reply<>(ReplyStatus.ERROR, "Product not found", null) :
                            new Reply<>(ReplyStatus.SUCCESS, "Read product name", productDTO);
                    }
                }
                case CommandType.READ_ALL -> {
                    log.info("Getting all products");
                    yield new Reply<>(ReplyStatus.SUCCESS, "Read all products", service.findAll());
                }
                case CommandType.UPDATE -> {
                    if( cmd.body() == null || cmd.id() == null ) {
                        log.warn("ID and body are required");
                        yield new Reply<>(ReplyStatus.ERROR, "ID and body are required", null);
                    } else {
                        ProductDTO productDTO = service.update(cmd.id(), cmd.body());

                        if( productDTO != null ){
                            log.info("Updating product: name={}, price={}", productDTO.name(), productDTO.price());
                            yield new Reply<>(ReplyStatus.SUCCESS, "Update product name", productDTO);
                        } else {
                            log.warn("Product not found, null Product DTO");
                            yield new Reply<>(ReplyStatus.ERROR, "Product not found", null);
                        }
                    }
                }
                case CommandType.DELETE -> {
                    if( cmd.id() == null ) {
                        log.warn("ID is required");
                        yield new Reply<>(ReplyStatus.ERROR, "ID is required", null);
                    } else {
                        boolean result = service.delete(cmd.id());

                        log.info("Deleting product: id={}", cmd.body().id());

                        yield (result) ?
                                new Reply<>(ReplyStatus.SUCCESS, "Deleting product", "deleted") :
                                new Reply<>(ReplyStatus.ERROR, "Product not found", null);
                    }
                }
                default -> {
                    log.warn("Unknown command type={}", cmd.type());
                    yield new Reply<>(ReplyStatus.ERROR, "Unknown command type", service.findAll());
                }
            };

            return MessageBuilder.withPayload(reply)
                    .setHeader("correlationId", correlationId)
                    .build();
        };
    }

}
