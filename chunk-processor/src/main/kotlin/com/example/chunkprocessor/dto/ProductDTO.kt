package com.example.lambda.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class ProductDTO(
    val code: Long,
    val brands: String?,
    val image_url: String?,
    val ingredients_analysis_tags: List<String>?,
    val countries_hierarchy: List<String>?,
    val allergens_hierarchy: List<String>?,
    val categories_tags: List<String>?,
    val additives_tags: List<String>?,
    val nutriments: NutrimentDTO?,
)