package com.jcandia.course.springcloud.kafka.command.models.mapper;

import com.jcandia.course.springcloud.kafka.command.entities.Product;
import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;

public final class Mappers {

    private Mappers() {}

    static public ProductDTO toDTO(Product product) {
        return new ProductDTO(product.getId(), product.getName(), product.getPrice());
    }

    static public Product toEntity(ProductDTO productDTO) {
        Product entity = new Product(productDTO.name(), productDTO.price());
        entity.setId(productDTO.id());
        return entity;
    }
}
