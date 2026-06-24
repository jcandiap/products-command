package com.jcandia.course.springcloud.kafka.command.handlers;

import com.jcandia.course.springcloud.kafka.command.models.Command;
import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductCommandConsumer.class);

    @Bean
    public Consumer<Command<ProductDTO>> handleCommands() {
        return cmd -> {
            log.info("Command recived successfully: type={}, body={}", cmd.type(), cmd.body());
        };
    }

}
