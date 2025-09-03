package com.example.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.example.lambda.dto.ProductDTO
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.*

class ProductImporter {

    private val objectMapper = jacksonObjectMapper()

    private val jdbcUrl = System.getenv("JDBC_URL")
        ?: error("JDBC_URL env var required")
    private val dbUser = System.getenv("DB_USER")
        ?: error("DB_USER env var required")
    private val dbPass = System.getenv("DB_PASSWORD")
        ?: error("DB_PASSWORD env var required")
    private val batchSize = System.getenv("BATCH_SIZE")?.toIntOrNull() ?: 50

    fun importJsonl(input: java.io.InputStream, context: Context): Int {
        context.logger.log("=== PRODUCT IMPORTER START ===\n")
        context.logger.log("Environment - JDBC_URL: ${jdbcUrl.take(50)}...\n")
        context.logger.log("Environment - DB_USER: $dbUser\n")
        context.logger.log("Environment - BATCH_SIZE: $batchSize\n")

        return try {
            context.logger.log("Attempting database connection...\n")
            DriverManager.getConnection(jdbcUrl, dbUser, dbPass).use { conn ->
                context.logger.log("✓ Database connected successfully\n")
                context.logger.log("Database URL: ${conn.metaData.url}\n")
                context.logger.log("Database Product: ${conn.metaData.databaseProductName} ${conn.metaData.databaseProductVersion}\n")

                conn.autoCommit = false
                context.logger.log("✓ Auto-commit disabled\n")
                Prepared(conn).use { p ->
                    var imported = 0
                    var lineCount = 0
                    var validJsonCount = 0
                    var processedCount = 0

                    context.logger.log("Starting to read input stream...\n")

                    input.bufferedReader(Charsets.UTF_8).useLines { lines ->
                        context.logger.log("Stream opened, processing lines...\n")

                        lines.forEach { line ->
                            lineCount++

                            if (lineCount <= 5) {
                                context.logger.log("Line $lineCount: ${line.take(100)}...\n")
                            } else if (lineCount % 1000 == 0) {
                                context.logger.log("Processed $lineCount lines so far\n")
                            }

                            if (line.isBlank()) {
                                context.logger.log("Skipping blank line $lineCount\n")
                                return@forEach
                            }

                            try {
                                val dto: ProductDTO = objectMapper.readValue(line)
                                validJsonCount++

                                if (validJsonCount <= 3) {
                                    context.logger.log("Successfully parsed JSON for line $lineCount: barcode=${dto.code}, name=${dto.brands?.take(30)}\n")
                                }

                                val barcode = dto.code

                                if (barcode == null || barcode <= 0) {
                                    context.logger.log("Skipping line $lineCount: invalid barcode ($barcode)\n")
                                    return@forEach
                                }

                                processedCount++

                                val name = dto.brands?.trim().takeUnless { it.isNullOrBlank() } ?: "Unknown"
                                val imageUrl = dto.image_url?.trim()

                                if (processedCount <= 3) {
                                    context.logger.log("Processing product: barcode=$barcode, name='$name', imageUrl='${imageUrl?.take(50)}'\n")
                                }

                                val tagIds = p.upsertNamesAndCollectIds(dto.ingredients_analysis_tags, Table.TAG)
                                val categoryIds = p.upsertNamesAndCollectIds(dto.categories_tags, Table.CATEGORY)
                                val allergenIds = p.upsertNamesAndCollectIds(dto.allergens_hierarchy, Table.ALLERGEN)
                                val countryIds = p.upsertNamesAndCollectIds(dto.countries_hierarchy, Table.COUNTRY)
                                val additiveIds = p.upsertNamesAndCollectIds(dto.additives_tags, Table.ADDITIVE)
                                val nutrimentId = p.insertNutriments(dto)

                                if (processedCount <= 3) {
                                    context.logger.log("Related IDs - tags: ${tagIds.size}, categories: ${categoryIds.size}, allergens: ${allergenIds.size}, countries: ${countryIds.size}, additives: ${additiveIds.size}, nutriment: $nutrimentId\n")
                                }

                                p.psInsertProduct.setLong(1, barcode)
                                p.psInsertProduct.setObject(2, nutrimentId)
                                p.psInsertProduct.setObject(3, null)
                                p.psInsertProduct.setString(4, name)
                                p.psInsertProduct.setString(5, imageUrl)
                                p.psInsertProduct.addBatch()

                                p.addLinkBatch(TableLink.PRODUCT_TAGS, barcode, tagIds)
                                p.addLinkBatch(TableLink.PRODUCT_CATEGORIES, barcode, categoryIds)
                                p.addLinkBatch(TableLink.PRODUCT_ALLERGENS, barcode, allergenIds)
                                p.addLinkBatch(TableLink.PRODUCT_COUNTRIES, barcode, countryIds)
                                p.addLinkBatch(TableLink.PRODUCT_ADDITIVES, barcode, additiveIds)

                                imported++

                                if (imported % batchSize == 0) {
                                    context.logger.log("Executing batch at $imported products...\n")
                                    val productResults = p.psInsertProduct.executeBatch()
                                    context.logger.log("Product batch executed: ${productResults.contentToString()}\n")

                                    p.flushLinkBatches()
                                    context.logger.log("Link batches flushed\n")

                                    conn.commit()
                                    context.logger.log("✓ Batch committed: $imported products\n")

                                    conn.createStatement().use { st ->
                                        st.executeQuery("SELECT COUNT(*) FROM product WHERE barcode != 9999999999999").use { rs ->
                                            rs.next()
                                            val actualCount = rs.getLong(1)
                                            context.logger.log("Database verification: $actualCount products in database\n")
                                        }
                                    }
                                }
                            } catch (parseError: Exception) {
                                if (lineCount <= 10) {
                                    context.logger.log("JSON parse error on line $lineCount: ${parseError.message}\n")
                                    context.logger.log("Line content: ${line.take(200)}\n")
                                }
                            }
                        }
                    }

                    context.logger.log("Stream processing complete. Stats: lines=$lineCount, validJson=$validJsonCount, processed=$processedCount, imported=$imported\n")

                    if (imported > 0) {
                        context.logger.log("Processing final batch...\n")
                        val productResults = p.psInsertProduct.executeBatch()
                        context.logger.log("Final product batch: ${productResults.contentToString()}\n")

                        p.flushLinkBatches()
                        context.logger.log("Final link batches flushed\n")

                        conn.commit()
                        context.logger.log("✓ Final batch committed\n")

                        conn.createStatement().use { st ->
                            st.executeQuery("SELECT COUNT(*) FROM product WHERE barcode != 9999999999999").use { rs ->
                                rs.next()
                                val finalCount = rs.getLong(1)
                                context.logger.log("FINAL DATABASE COUNT: $finalCount products\n")
                            }
                        }
                    } else {
                        context.logger.log("WARNING: No products were imported!\n")
                    }

                    context.logger.log("=== PRODUCT IMPORTER END ===\n")
                    return imported
                }
            }
        } catch (e: Exception) {
            context.logger.log("FATAL ERROR in importJsonl: ${e.javaClass.simpleName}: ${e.message}\n")
            e.printStackTrace()
            throw e
        }
    }

    private enum class Table(val table: String) {
        TAG("tag"), CATEGORY("category"), ALLERGEN("allergen"), COUNTRY("country"), ADDITIVE("additive")
    }
    private enum class TableLink(val table: String) {
        PRODUCT_TAGS("product_tags"),
        PRODUCT_CATEGORIES("product_categories"),
        PRODUCT_ALLERGENS("product_allergens"),
        PRODUCT_COUNTRIES("product_countries"),
        PRODUCT_ADDITIVES("product_additives")
    }

    private inner class Prepared(conn: Connection) : AutoCloseable {
        private val psUpsertName: Map<Table, PreparedStatement> = Table.values().associateWith { tbl ->
            conn.prepareStatement(
                "INSERT INTO ${tbl.table}(name) VALUES (?) ON CONFLICT (name) DO NOTHING"
            )
        }
        private val psSelectIdByName: Map<Table, PreparedStatement> = Table.values().associateWith { tbl ->
            conn.prepareStatement("SELECT id FROM ${tbl.table} WHERE name = ?")
        }

        private val psInsertNutriments = conn.prepareStatement(
            """
            INSERT INTO nutriments(
                carbohydrates, carbohydrates_100g, energy, energy_kj_100g, fat, fat_100g,
                fiber, fiber_100g, proteins, proteins_100g, salt, salt_100g,
                saturated_fat, saturated_fat_100g, sodium, sodium_100g, sugars, sugars_100g
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            RETURNING id
            """.trimIndent()
        )

        val psInsertProduct: PreparedStatement = conn.prepareStatement(
            """
            INSERT INTO product(barcode, nutriment_id, nutriscore_id, name, image_url)
            VALUES (?,?,?,?,?)
            ON CONFLICT (barcode) DO UPDATE SET
                name = EXCLUDED.name,
                image_url = EXCLUDED.image_url,
                nutriment_id = COALESCE(EXCLUDED.nutriment_id, product.nutriment_id)
            """.trimIndent()
        )

        private val psInsertLink: Map<TableLink, PreparedStatement> = mapOf(
            TableLink.PRODUCT_TAGS to conn.prepareStatement(
                "INSERT INTO product_tags(product_id, tag_id) VALUES (?,?) ON CONFLICT DO NOTHING"
            ),
            TableLink.PRODUCT_CATEGORIES to conn.prepareStatement(
                "INSERT INTO product_categories(product_id, category_id) VALUES (?,?) ON CONFLICT DO NOTHING"
            ),
            TableLink.PRODUCT_ALLERGENS to conn.prepareStatement(
                "INSERT INTO product_allergens(product_id, allergen_id) VALUES (?,?) ON CONFLICT DO NOTHING"
            ),
            TableLink.PRODUCT_COUNTRIES to conn.prepareStatement(
                "INSERT INTO product_countries(product_id, country_id) VALUES (?,?) ON CONFLICT DO NOTHING"
            ),
            TableLink.PRODUCT_ADDITIVES to conn.prepareStatement(
                "INSERT INTO product_additives(product_id, additive_id) VALUES (?,?) ON CONFLICT DO NOTHING"
            )
        )

        fun upsertNamesAndCollectIds(names: List<String>?, table: Table, lang: String = "en"): List<Long> {
            val filtered = normalizeLangPrefixedNames(names, lang)
            if (filtered.isEmpty()) return emptyList()

            val ids = ArrayList<Long>(filtered.size)
            val up = psUpsertName.getValue(table)
            val sel = psSelectIdByName.getValue(table)

            for (name in filtered) {
                up.setString(1, name)
                up.addBatch()
            }
            up.executeBatch()

            for (name in filtered) {
                sel.setString(1, name)
                sel.executeQuery().use { rs ->
                    if (rs.next()) ids.add(rs.getLong(1))
                }
            }
            return ids
        }


        fun addLinkBatch(link: TableLink, productBarcode: Long, ids: List<Long>) {
            if (ids.isEmpty()) return
            val ps = psInsertLink.getValue(link)
            for (id in ids) {
                ps.setLong(1, productBarcode)
                ps.setLong(2, id)
                ps.addBatch()
            }
        }

        fun insertNutriments(dto: ProductDTO): Long? {
            val n = dto.nutriments ?: return null
            val vals = arrayOf(
                n.carbohydrates, n.carbohydrates_100g, n.energy, n.energy_kj_100g, n.fat, n.fat_100g,
                n.fiber, n.fiber_100g, n.proteins, n.proteins_100g, n.salt, n.salt_100g,
                n.saturated_fat, n.saturated_fat_100g, n.sodium, n.sodium_100g, n.sugars, n.sugars_100g
            )
            for ((i, v) in vals.withIndex()) psInsertNutriments.setObject(i + 1, v)
            psInsertNutriments.executeQuery().use { rs -> if (rs.next()) return rs.getLong(1) }
            return null
        }

        fun flushLinkBatches() {
            psInsertLink.values.forEach { it.executeBatch() }
        }

        override fun close() {
            psInsertProduct.close()
            psInsertLink.values.forEach { it.close() }
            psUpsertName.values.forEach { it.close() }
            psSelectIdByName.values.forEach { it.close() }
            psInsertNutriments.close()
        }
    }

    private fun normalizeLangPrefixedNames(
        names: List<String>?,
        lang: String = "en"
    ): List<String> {
        if (names.isNullOrEmpty()) return emptyList()
        val prefix = "${lang.lowercase()}:"
        return names.asSequence()
            .map { it.trim().lowercase() }
            .filter { it.isNotEmpty() && it.startsWith(prefix) }
            .map { it.removePrefix(prefix).trim() }
            .filter { it.isNotEmpty() }
            .distinct()
            .toList()
    }

}