package com.jcandia.course.springcloud.kafka.command.services;

import com.jcandia.course.springcloud.kafka.command.entities.Product;
import com.jcandia.course.springcloud.kafka.command.models.dto.ProductDTO;
import com.jcandia.course.springcloud.kafka.command.models.mapper.Mappers;
import com.jcandia.course.springcloud.kafka.command.repositories.ProductRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    private final ProductRepository productRepository;

    public ProductServiceImpl(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Override
    @Transactional
    public ProductDTO create(ProductDTO productDTO) {
        Product productNew = productRepository.save(Mappers.toEntity(productDTO));
        return Mappers.toDTO(productNew);
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDTO findById(Long id) {
        return productRepository.findById(id).map(Mappers::toDTO).orElse(null);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ProductDTO> findAll() {
        return ((List<Product>)productRepository.findAll()).stream()
                .map(Mappers::toDTO)
                .toList();
    }

    @Override
    @Transactional
    public ProductDTO update(Long id, ProductDTO productDTO) {
        Product entity = productRepository.findById(id).orElse(null);
        if( entity == null ) {
            return null;
        }

        entity.setName(productDTO.name());
        entity.setPrice(productDTO.price());

        return Mappers.toDTO(productRepository.save(entity));
    }

    @Override
    @Transactional
    public boolean delete(Long id) {
        boolean result = productRepository.existsById(id);
        if( result ) {
            productRepository.deleteById(id);
            return true;
        }
        return false;
    }

}
