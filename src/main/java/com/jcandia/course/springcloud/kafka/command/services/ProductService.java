package com.jcandia.course.springcloud.kafka.command.services;

import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;

import java.util.List;

public interface ProductService {

    ProductDTO create(ProductDTO productDTO);
    ProductDTO findById(Long id);
    List<ProductDTO> findAll();
    ProductDTO update(Long id, ProductDTO productDTO);
    boolean delete(Long id);

}
