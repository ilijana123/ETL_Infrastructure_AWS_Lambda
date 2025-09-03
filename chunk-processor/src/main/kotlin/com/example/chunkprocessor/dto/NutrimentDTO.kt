package com.example.lambda.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class NutrimentDTO(
    val carbohydrates: Double? = null,
    val carbohydrates_100g: Double? = null,
    val energy: Double? = null,
    val energy_kj_100g: Double? = null,
    val fat: Double? = null,
    val fat_100g: Double? = null,
    val fiber: Double? = null,
    val fiber_100g: Double? = null,
    val proteins: Double? = null,
    val proteins_100g: Double? = null,
    val salt: Double? = null,
    val salt_100g: Double? = null,
    val saturated_fat: Double? = null,
    val saturated_fat_100g: Double? = null,
    val sodium: Double? = null,
    val sodium_100g: Double? = null,
    val sugars: Double? = null,
    val sugars_100g: Double? = null
)
