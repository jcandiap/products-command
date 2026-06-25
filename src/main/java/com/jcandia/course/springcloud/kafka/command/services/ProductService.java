package com.jcandia.course.springcloud.kafka.command.services;

import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;

public interface ProductService {

    ProductDTO create(ProductDTO productDTO);
    ProductDTO findById(Long id);

}
